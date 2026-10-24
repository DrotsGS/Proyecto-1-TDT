# DFSha — Especificación del servicio

**Proyecto 1, SI3007 Sistemas Distribuidos, 2026-2 — Universidad EAFIT**
**Opción asignada:** 1 (C/S con composición y distribución del servicio, S2S)
**Referencia de diseño:** HDFS / GFS (DFS por bloques)

Entregable de la semana 7 ("especificación definitiva por equipo"),
actualizado al estado final del proyecto.

---

## 1. Descripción del servicio

DFSha es un sistema de archivos distribuido **por bloques** para archivos
grandes. Un cliente ve un árbol jerárquico tipo Linux (`/docs/2026/a.pdf`)
y puede crear directorios, subir, bajar, leer por partes y borrar
archivos. Por debajo, cada archivo se parte en bloques de 128 MB, cada
bloque se guarda en tres máquinas distintas, y los metadatos viven en un
grupo de tres NameNodes replicados con Raft.

El problema que resuelve: guardar archivos más grandes que un disco, a
más velocidad que una máquina, sin perderlos cuando se cae una máquina y
sin que nadie más pueda leerlos.

**Semántica WORM (D2).** Un archivo se escribe una vez, de principio a
fin, y después solo se lee o se borra. No hay actualización parcial. Es la
misma decisión de HDFS y de los DFS basados en objetos que describe el
enunciado: los bloques inmutables eliminan la reconciliación entre
réplicas, que es el problema más difícil de la consistencia (RNF3).

## 2. Requisitos funcionales

| RF | Enunciado | Cómo se cumple | Dónde |
|---|---|---|---|
| RF1 | Gestión: `ls, cd, mkdir, rmdir, rm, ...` | `ls`, `cd`, `pwd`, `mkdir`, `rmdir`, `rm`, `stat`, `chmod`, con rutas absolutas o relativas al `cd` | `src/client/cli.py`, `NameNodeService` |
| RF2 | Transferencia: `send()`, `receive()` | `put` y `get` en la CLI; `send()` y `receive()` en el SDK | `cmd_put`, `cmd_get`, `DFSha.send/receive` |
| RF3 | Acceso: `open, close, read, write, lock` | `DFSha.open(path, "r"/"w")` devuelve un archivo con `read`, `seek`, `tell`, `write`, `close` y `lock`. `send`/`receive` están implementados **sobre** estas primitivas, como sugiere el enunciado | `src/client/sdk.py` |

Las operaciones básicas del ejemplo del enunciado (`put`, `get`, `ls`,
`rm`, `mkdir`, `rmdir`, usuario y contraseña, cada cliente con sus
propios archivos, API de lectura y escritura) están todas.

**RF3 bajo WORM.** `open(path, "w")` toma el *lock*: el lease de escritor
único. Mientras dura, nadie más puede crear ese path y el archivo es
invisible para `ls`. `write()` agrega al final (bloque a bloque con
`AddBlock`, sin conocer el tamaño final) y `close()` hace el commit. La
lectura sí es de acceso aleatorio: `seek()` + `read()` bajan solo el
bloque que contiene el offset.

## 3. Requisitos no funcionales

| RNF | Enunciado | Cómo se cumple |
|---|---|---|
| RNF1 Escalabilidad | usuarios, archivos, tamaño | Los datos no pasan por el NameNode (D3): el cliente habla directo con los DataNodes. Agregar DataNodes agrega capacidad y ancho de banda. Hash consistente (D10): un nodo nuevo mueve solo ~1/N de las claves (medido: 17.4 %) |
| RNF2 Alta disponibilidad | redundancia, sin caídas ni pérdida | Datos: 3 réplicas en máquinas distintas, re-replicación automática (medido: ~4–9 s tras morir un nodo). Control: 3 NameNodes con Raft, un líder nuevo en 1–3 s |
| RNF3 Consistencia | el reto de la replicación | Metadatos: log de Raft, un solo orden de operaciones, se lee solo del líder. Datos: bloques inmutables (WORM) con sha256 verificado en cada copia; un archivo o no existe o existe completo |
| RNF4 Particionamiento | archivos entre nodos, y cada archivo entre nodos | Bloques de 128 MB repartidos por hash consistente con 150 nodos virtuales |
| RNF5 Rendimiento | concurrencia en archivos y dentro de un archivo | Los bloques de un archivo suben y bajan en paralelo (`ThreadPoolExecutor`); el NameNode atiende con un pool de hilos; muchos clientes escriben archivos distintos a la vez |
| RNF6 Seguridad | cifrado en tránsito y en reposo, control de acceso, seguridad entre nodos | AES-256-GCM en el cliente (D19); mTLS en los tres planos (D20); usuario y contraseña con tokens HMAC; dueño, grupo y permisos por archivo (D17); tokens de bloque firmados (D18); tráfico de Raft cifrado |
| RNF7 Transparencia | de acceso y de localización, nunca estática | Localización: total, el cliente nunca sabe en qué DataNode ni cuál NameNode atiende (el `Lider` lo encuentra solo). Acceso: relajada a CLI + SDK, como HDFS (D3), nunca estática |

## 4. Arquitectura

**Opción 1: C/S con el servidor implementado por composición y
distribución (S2S), autónomo.** Patrón maestro–trabajador (D1).

```
                              CLIENTE  (CLI / SDK)
                     ┌───────────┴────────────┐
       plano de control                        plano de datos
       metadatos, permisos                     bloques cifrados
       gRPC + mTLS                             gRPC streaming + mTLS
                     │                         │
        ┌────────────▼────────────┐   ┌────────▼───────┐
        │ NameNode 1 (líder)      │   │ DataNode 1     │──┐ pipeline
        │ NameNode 2   ◄─ Raft ─► │   │ DataNode 2     │◄─┘ DN→DN
        │ NameNode 3              │   │ DataNode 3..N  │
        └────────────▲────────────┘   └────────┬───────┘
                     └── heartbeat, block report (DataNode → NameNode)
                         comandos piggyback en la respuesta
```

| Componente | Responsabilidad | Código |
|---|---|---|
| Cliente | Parte y reensambla archivos, cifra, habla con el líder y con los DataNodes | `src/client/cli.py`, `src/client/sdk.py` |
| NameNode (ControlNode) | Namespace, permisos, leases, colocación, re-replicación, recolector | `src/namenode/server.py`, `namespace.py`, `estado.py` |
| DataNode | Guarda bloques, valida tokens, reenvía por el pipeline, obedece comandos | `src/datanode/server.py` |

**Por qué no la opción 2 (P2P con SuperPeers).** Exigiría metadatos
distribuidos por DHT y una red overlay entre organizaciones. El diseño
tiene una autoridad única de metadatos —replicada, pero una— que es
maestro–trabajador puro.

## 5. Modelo de datos

**Namespace** (replicado por Raft): árbol de `Node` con nombre, tipo,
tamaño, estado (`UNDER_CONSTRUCTION` → `COMMITTED`), lista ordenada de
`block_id`, lease y su vencimiento, dueño, grupo y modo.

**Block map** (replicado por Raft): `block_id → {índice, tamaño, sha256,
destinos iniciales}`.

**Ubicaciones reales** (estado blando, NO replicado): qué DataNode tiene
cada bloque. Se reconstruye con `BlockReport` y `BlockReceived` (D11,
D15).

**En cada DataNode:** un archivo `blk_<block_id>` por bloque, escrito por
`tmp_` + renombrado atómico; su contenido es la secuencia de chunks
cifrados.

## 6. Algoritmos de particionamiento y distribución

1. **Partición.** El NameNode, en `Create`, corta el tamaño en bloques de
   `BLOCK_SIZE` con un contador explícito (el último es parcial). Con
   `AddBlock` el cliente pide bloques uno a uno.
2. **Colocación.** `ConsistentHashPlacer`: el `block_id` cae en un anillo
   con 150 nodos virtuales por DataNode vivo; se toman los 3 primeros
   nodos **físicos distintos** en sentido horario. El hash es política de
   colocación inicial, no mecanismo de búsqueda: el NameNode recuerda
   dónde quedó cada bloque.
3. **Escritura.** Pipeline cliente → DN1 → DN2 → DN3 (D13).
4. **Lectura.** El cliente baja los bloques en paralelo de cualquier
   réplica viva y salta a la siguiente si una falla o llega corrupta.
5. **Re-replicación.** Cada 5 s el líder busca bloques con menos de 3
   copias vivas y le pide a un DataNode que tenga una que la copie a los
   que faltan (comando piggyback).

## 7. Comunicaciones

gRPC con Protocol Buffers sobre HTTP/2 y mTLS en todos los pares. Detalle
completo en `docs/protocolos.md`.

| Par | Protocolo |
|---|---|
| Cliente ↔ ControlNode | gRPC unario, `NameNodeService` |
| Cliente ↔ DataNode | gRPC con streaming, `DataNodeService` |
| ControlNode ↔ ControlNode | Raft (pysyncobj) sobre TCP cifrado |
| ControlNode ↔ DataNode | gRPC unario iniciado SIEMPRE por el DataNode; comandos piggyback |
| DataNode ↔ DataNode | gRPC streaming, `PutBlock` con pipeline |

## 8. Decisiones de diseño

Las 20 decisiones, con qué se decidió, por qué, qué se descartó y a qué
obliga, están en `docs/decisiones.md`. Las de mayor peso:

- **D2** WORM: elimina la reconciliación entre réplicas.
- **D3** el cliente particiona: el NameNode fuera del camino de los datos.
- **D6** `rm` gana sobre una subida en curso: un path nunca queda preso.
- **D11/D15** ubicaciones como estado blando reconstruido por reports.
- **D14** Raft y lectura solo del líder: consistencia sobre disponibilidad.
- **D19** cifrado en el cliente: ni los DataNodes ven texto claro.

## 9. Entorno de ejecución

Python 3.12 (Docker) / 3.14 (Windows), gRPC 1.83, pysyncobj 0.3.14,
cryptography 50. Cada nodo corre nativo o en un contenedor Docker, en
máquinas distintas (AWS Academy, una VM por nodo) o en una sola. Ver
`docs/despliegue.md`.

## 10. Plan de trabajo

| Semana | Hito del curso | Qué se hizo |
|---|---|---|
| 6 | enunciado | contrato gRPC, esqueleto de los tres nodos, cinco costuras |
| 7 | especificación | RF1 completo, FileAuth, este documento |
| 8 | hito 1: C/S con RF1 y RF2 | `put`/`get` por streaming, verificado con 500 MB |
| 9 | — | despliegue preparado, seguridad adelantada |
| 10 | hito 2: distribuido + comunicaciones | hash consistente, nodos muertos, block reports, paralelismo, `docs/protocolos.md` |
| 11 | — | replicación ×3 por pipeline, recolector, ACL, `cd` |
| 12 | hito 3: HA, replicación, consistencia, seguridad | Raft, re-replicación, leases, mTLS, tokens, AES-GCM, SDK (RF3) |
| 13 | entrega | mediciones, informe, video |
