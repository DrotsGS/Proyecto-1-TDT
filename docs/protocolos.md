# DFSha — Especificación de protocolos y APIs

Entregable de la semana 10 ("especificación de comunicaciones"),
actualizado con lo que agregaron las semanas 11 y 12. El contrato
completo está en `src/proto/` (cuatro archivos, D12):

| Archivo | Contenido |
|---|---|
| `dfsha_common.proto` | `Empty`, `StatusResponse`, `FileState`, `Entry` |
| `dfsha_namenode.proto` | `NameNodeService`: cliente ↔ ControlNode |
| `dfsha_datanode.proto` | `DataNodeService`: cliente ↔ DataNode y DataNode ↔ DataNode |
| `dfsha_control.proto` | `ControlService`: DataNode → ControlNode |

**Transporte común.** gRPC sobre HTTP/2 con Protocol Buffers. Con
`DFSHA_TLS=1`, mTLS en todos los pares (D20). Toda dirección sale de
variables de entorno; ninguna está en el código.

**¿Por qué gRPC y no REST?** El plano de datos mueve bloques de 128 MB:
gRPC da streaming bidireccional nativo con control de flujo, y Protocol
Buffers serializa los bytes sin base64. Además el contrato tipado
(`.proto`) genera los stubs y obliga a los tres programas a hablar
exactamente el mismo idioma. Los códigos de estado de gRPC
(`NOT_FOUND`, `PERMISSION_DENIED`, …) dan una semántica de error más
precisa que los de HTTP para un sistema de archivos.

---

## 1. Cliente ↔ ControlNode (NameNode)

**Propósito.** El plano de control: el namespace, los permisos y dónde
están o dónde poner los bloques. Por aquí no pasa un byte de los
archivos.

**Protocolo.** gRPC unario (`NameNodeService`). Mensajes pequeños,
pedido–respuesta: no hace falta streaming.

**Quién inicia.** El cliente. Solo el **líder** de Raft atiende; un
seguidor responde `UNAVAILABLE` ("no soy el líder: <dirección>"). El
cliente (`client.cli.Lider`) prueba las direcciones de `NAMENODE_ADDR`
con `Ping` hasta dar con `is_leader = true` y reintenta ante
`UNAVAILABLE` (hasta 8 veces, 1 s entre intentos): es lo que hace
transparente un cambio de líder.

| RPC | Pide | Responde | Para qué |
|---|---|---|---|
| `Login` | `user`, `password` | `token`, `expires_at` | autenticación (la atiende cualquier NameNode) |
| `Ping` | — | `node_id`, `is_leader`, `leader_addr` | descubrir al líder |
| `Ls` | `path`, `token` | `entries[]` (nombre, tipo, tamaño, dueño, grupo, modo) | RF1 |
| `Mkdir` / `Rmdir` / `Rm` | `path`, `token` | `ok`, `message` | RF1 |
| `Stat` | `path`, `token` | `FileInfo` (incluye estado, dueño, modo) | RF1, diagnóstico |
| `Chmod` | `path`, `mode`, `group`, `token` | `ok` | RNF6 |
| `Create` | `path`, `size`, `token` | `lease_id`, `blocks[]` (id, índice, tamaño, DataNodes, token de escritura), `block_size`, `lease_expires_at` | RF2: empieza una escritura y toma el lock |
| `AddBlock` | `lease_id`, `size`, `token` | un `BlockAssignment` | RF3: escribir sin conocer el tamaño |
| `Renew` | `lease_id`, `token` | `ok` | mantener vivo el lock |
| `Complete` | `path`, `lease_id`, `checksums[]` | `ok` | commit: el archivo se vuelve visible e inmutable |
| `Abort` | `lease_id`, `token` | `ok` | cancela y libera el path |
| `Open` | `path`, `token` | `size`, `blocks[]` (id, tamaño, réplicas vivas, sha256, token de lectura) | RF2/RF3: lectura |

**Secuencia de escritura (`put`).**

```
Cliente                 NameNode líder            NameNode seguidores
  │ Create(path,size) ──►│                              │
  │                      │── op "create" por Raft ─────►│ (mayoría confirma)
  │◄── lease + bloques ──│                              │
  │   ...sube los bloques al plano de datos (par 2)...  │
  │ Renew (cada TTL/3) ─►│── op "renew" ───────────────►│
  │ Complete(checksums) ►│── op "complete" ────────────►│
  │◄──────── ok ─────────│                              │
```

**Errores** (tabla única en `src/namenode/server.py`):

| Situación | `StatusCode` |
|---|---|
| Token inválido o vencido | `UNAUTHENTICATED` |
| Sin permiso sobre el path, lease ajeno, `chmod` a un grupo ajeno | `PERMISSION_DENIED` |
| El path, el padre o el lease no existen | `NOT_FOUND` |
| Ya existe (`mkdir` duplicado, `Create` sobre algo existente: WORM) | `ALREADY_EXISTS` |
| Directorio no vacío; borrar la raíz; lease inválido; faltan checksums; abrir algo en construcción | `FAILED_PRECONDITION` |
| `rmdir` de un archivo, `rm` de un directorio, `Open` de un directorio, bloque de tamaño inválido | `INVALID_ARGUMENT` |
| No soy el líder; sin mayoría; ningún DataNode vivo o con el bloque | `UNAVAILABLE` |
| Cualquier otro | `INTERNAL` |

---

## 2. Cliente ↔ DataNode

**Propósito.** El plano de datos: los bytes de los bloques, cifrados por
el cliente (D19).

**Protocolo.** gRPC **streaming**:

- `PutBlock(stream BlockChunk) → PutBlockResponse`: client streaming. El
  primer mensaje es un `BlockHeader` (id, tamaño cifrado, token,
  pipeline); los siguientes, `data` de hasta 1 MB + 28 bytes.
- `GetBlock(GetBlockRequest) → stream BlockChunk`: server streaming.

Streaming porque un bloque de 128 MB no cabe cómodo en un mensaje ni en
memoria: ambos lados escriben a disco a medida que llega.

**Quién inicia.** El cliente, con las direcciones que le dio el NameNode.

**Seguridad.** Cada operación lleva un `access_token` firmado por el
NameNode (`w` para escribir, `r` para leer), que el DataNode valida solo
con el secreto compartido (D18).

**Secuencia de lectura (`get`).**

```
Cliente                     NameNode              DataNode A    DataNode B
  │ Open(path) ────────────►│                         │             │
  │◄── bloques, réplicas,───│                         │             │
  │    sha256, tokens "r"   │                         │             │
  │ GetBlock(b0, token) ───────────────────────────►  │  (en paralelo│
  │◄══════ chunks cifrados ════════════════════════   │   por bloque)│
  │   si A falla o el sha256 no coincide ───────────────────────────►│
  │◄═══════════════════════════════════════════════════════════════  │
  │   descifra con AES-GCM y escribe en su offset                    │
```

**Errores:** `PERMISSION_DENIED` (token inválido, vencido, de otra
operación o de otro bloque); `NOT_FOUND` (no tiene el bloque);
`FAILED_PRECONDITION` (llegaron más o menos bytes que los anunciados: no
queda nada en disco); `INVALID_ARGUMENT` (stream sin header o con dos).

---

## 3. ControlNode ↔ ControlNode (NameNode ↔ NameNode)

**Propósito.** Que los tres NameNodes tengan exactamente el mismo
namespace, en el mismo orden, aunque se caiga cualquiera (RNF2, RNF3).

**Protocolo.** **Raft**, con la biblioteca `pysyncobj`, sobre TCP en el
puerto de Raft (50052) y cifrado con `RAFT_PASSWORD`. No es gRPC: Raft
tiene su propio protocolo de mensajes (`request_vote`,
`append_entries`), y implementarlo a mano sobre gRPC es donde están los
errores sutiles (D14).

**Qué viaja.** Las operaciones que cambian el estado durable: `mkdir`,
`rmdir`, `rm`, `chmod`, `create`, `add_block`, `renew`, `complete`,
`abort`, `expirar`. Cada una lleva todo lo no determinista ya decidido
por el líder (uuids, hora, colocación).

**Quién inicia.** El líder replica; un candidato pide votos. Elección con
timeouts aleatorios de 0.4–1.4 s. Un líder que pasa 3 s sin la mayoría se
retira.

**Fallas.** Con 1 de 3 caído, todo sigue. Con 2 de 3 caídos, no se
escribe (`UNAVAILABLE`): Raft elige consistencia. Cada NameNode guarda el
log en disco y recupera el namespace al reiniciar.

---

## 4. ControlNode ↔ DataNode

**Propósito.** Que los NameNodes sepan qué DataNodes están vivos y qué
bloques tienen, y que el líder pueda pedirles borrar o copiar bloques.

**Protocolo.** gRPC unario (`ControlService`), **iniciado siempre por el
DataNode**. El NameNode nunca abre una conexión hacia un DataNode.

| RPC | Cuándo | Para qué |
|---|---|---|
| `Heartbeat(node_id, addr, free_bytes, num_blocks)` | cada 3 s | estar vivo; anunciar su dirección; recibir comandos |
| `BlockReport(node_id, block_ids[])` | al arrancar y cada 60 s | reconstruir el mapa de ubicaciones (D11) |
| `BlockReceived(node_id, block_id, sha256)` | al guardar un bloque | confirmar una réplica al instante |

**Comandos piggyback.** La respuesta al heartbeat (`HeartbeatResponse`)
lleva `commands[]`: `ReplicateCommand(block_id, targets, token)` o
`DeleteCommand(block_ids)`, más `is_leader`. El DataNode solo obedece al
líder.

**Por qué el DataNode inicia (lo menos obvio del diseño).** Resuelve NAT y
firewalls: el NameNode no necesita poder alcanzar a los DataNodes, solo
al revés. Agregar un DataNode no exige tocar el NameNode: basta con que
empiece a latir. Y el NameNode nunca bloquea un hilo esperando a un
DataNode lento.

**A todos los NameNodes.** Cada DataNode late y reporta a los tres, cada
uno en su propio hilo (D15): el líder nuevo ya sabe todo al ser elegido,
y un NameNode caído no atrasa el latido hacia el líder.

**Fallas.** Un DataNode sin latir 30 s se da por muerto: no recibe
bloques nuevos, no aparece en `Open`, y sus bloques se re-replican.

---

## 5. DataNode ↔ DataNode

**Propósito.** Replicar un bloque sin que el cliente lo suba tres veces.

**Protocolo.** El mismo `PutBlock` en streaming. Si el `BlockHeader` trae
`pipeline = [DN2, DN3]`, DN1 guarda el bloque y lo reenvía a DN2 con
`pipeline = [DN3]`, y así (D13). La respuesta sube con las réplicas que
de verdad quedaron. Si un nodo de la cadena no responde, se salta.

`ReplicateTo(block_id, targets, token)` (y el comando piggyback
equivalente) copia un bloque que el DataNode ya tiene: es la
re-replicación.

**Quién inicia.** El DataNode que tiene el bloque. Usa el mismo token de
escritura que recibió (pipeline) o el que firmó el NameNode en el comando
(re-replicación).

---

## API para programas (SDK)

`src/client/sdk.py` expone lo mismo como biblioteca de Python:

```python
from client.sdk import DFSha
fs = DFSha(["nn1:50051", "nn2:50051", "nn3:50051"])
fs.login("drots", "dfsha")
fs.mkdir("/docs")
with fs.open("/docs/a.txt", "w") as f:   # lock
    f.write(b"...")
with fs.open("/docs/a.txt") as f:
    f.seek(100); f.read(50)
fs.send("local.bin", "/docs/b.bin"); fs.receive("/docs/b.bin", "copia.bin")
```
