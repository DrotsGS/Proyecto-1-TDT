# Registro de decisiones

Una entrada por decision. Esto se convierte solo en la seccion de
justificacion arquitectonica del informe final, que vale nota.

Formato: que decidi, por que, que descarte, y a que me obliga.

---

## D1 - Opcion 1: C/S con distribucion S2S

**Semana:** 6
**Decision:** arquitectura cliente/servidor con el servicio distribuido por
composicion, no P2P con SuperPeers.
**Por que:** el diseno tiene un NameNode central con autoridad unica sobre los
metadatos, que es master-worker puro. La opcion 2 habria exigido metadatos
distribuidos, lookup por DHT y red overlay entre organizaciones.
**Descartado:** P2P con SuperPeers.
**Me obliga a:** replicar el NameNode, porque un master unico es punto unico
de falla y RNF2 pide alta disponibilidad.

## D2 - WORM en vez de CRUD

**Semana:** 6
**Decision:** los bloques son inmutables una vez escritos. CRUD a nivel de
archivo (crear, leer, borrar), WORM a nivel de bloque.
**Por que:** con bloques de 128 MB, actualizar un byte obliga a reescribir el
bloque completo y sus tres replicas. Bloques inmutables eliminan la
reconciliacion entre replicas.
**Descartado:** CRUD con escritura aleatoria.
**Me obliga a:** modelar el estado UNDER_CONSTRUCTION -> COMMITTED, y explicar
como se cubre RF3: `lock()` pasa a ser un lease de escritor unico durante la
creacion, y `write()` solo existe mientras el archivo esta en construccion.

## D3 - El cliente particiona y reensambla

**Semana:** 6
**Decision:** el cliente parte el archivo en bloques y los reensambla. El
NameNode nunca ve un byte de datos.
**Por que:** saca al NameNode del camino de los datos. Si los bytes pasaran por
el, el ancho de banda del sistema estaria limitado por una sola maquina y se
caen RNF1 y RNF5.
**Descartado:** que el servidor particione.
**Me obliga a:** relajar la transparencia de acceso. Se usa CLI y SDK propio
en vez de montar en el sistema operativo, igual que HDFS. La transparencia de
localizacion si es total: el cliente pide las ubicaciones en cada operacion.

## D4 - Un solo .proto en la semana 6

**Semana:** 6
**Decision:** los tres servicios en un solo archivo.
**Por que:** menos friccion de generacion de codigo mientras el contrato
todavia se mueve.
**Descartado:** tres archivos separados desde el inicio.
**Me obliga a:** separarlo en la semana 10, cuando haya varios DataNodes.

## D5 - `rmdir` no recursivo por defecto

**Semana:** 7
**Decision:** `rmdir` sobre un directorio con hijos falla con
`FAILED_PRECONDITION`. El borrado recursivo existe en la firma
(`rmdir(path, recursivo=False)`) pero el protocolo todavia no tiene como
pedirlo.
**Por que:** HDFS exige `-r` explicito por la misma razon: el borrado
recursivo silencioso es la forma mas rapida de perder datos. Ademas evita
tocar el `.proto` en la semana 7, cuando el tiempo estaba contra el hito 1.
**Descartado:** borrado recursivo implicito.
**Me obliga a:** agregar el flag a `PathRequest` cuando se quiera exponer.
La logica del namespace no cambia, solo el mensaje.

## D6 - `rm` sobre un archivo en construccion

**Semana:** 7
**Decision:** el `rm` siempre gana. Borra el nodo, el lease muere con el y
los bloques ya escritos se agendan para borrado. El cliente que estuviera
subiendo se entera cuando su `Complete` falle con `FAILED_PRECONDITION`.
**Por que:** tres razones. (1) Es lo que hace HDFS. (2) Conecta con
`HeartbeatResponse.commands`, que ya estaba en el contrato desde la semana
6: le da razon de ser a un mecanismo ya disenado. (3) Un path no puede
quedar bloqueado para siempre por un cliente que se murio, y eso importa
mas que dejar bloques huerfanos un rato.
**Descartado:** rechazar el `rm` mientras el lease este vivo. Es mas simple
y no deja huerfanos, pero si el cliente muere y el lease nunca expira, ese
path queda preso hasta que exista expiracion de leases o un `abort` manual.
**Me obliga a:** reconocer la deuda: los bloques huerfanos ocupan disco
hasta que el recolector corra en la semana 11. La cola
`pendientes_borrado` ya los acumula y `BlockReport` tambien detecta los que
ningun archivo reclama.

## D7 - Metadatos en memoria hasta Raft

**Semana:** 8
**Decision:** el namespace y el block map viven solo en memoria. No hay
persistencia a disco.
**Por que:** en la semana 12 los reemplaza el log replicado de Raft.
Escribir ahora un formato de persistencia que se va a botar es trabajo
perdido en las semanas mas cargadas del proyecto.
**Descartado:** volcar el namespace a JSON en cada `Complete`. Si llega a
hacer falta antes de la semana 12, es una hora de trabajo, no seis.
**Me obliga a:** decir claramente que reiniciar el NameNode hoy pierde el
namespace, aunque no pierde los bloques: siguen en disco de los DataNodes
y `BlockReport` los vuelve a anunciar (ver D11).

## D8 - Cliente muerto entre `Create` y `Complete`

**Semana:** 8
**Decision:** el archivo queda `UNDER_CONSTRUCTION`, invisible para `ls`, y
un `rm` lo limpia (D6). El cliente, si sigue vivo, llama a `Abort` el.
**Por que:** es la consecuencia natural de D6 y no necesita maquinaria
nueva. El cliente ya hace `Abort` cuando una subida falla a mitad.
**Descartado:** expiracion automatica de leases por tiempo.
**Me obliga a:** implementar esa expiracion en la semana 12, cuando el
NameNode tenga a Raft debajo y un reloj comun entre replicas.

## D9 - Bloques en serie en el hito 1

**Semana:** 8
**Decision:** el cliente sube y baja los bloques uno por uno.
**Por que:** con un solo DataNode el paralelismo no compra nada, y en serie
es mucho mas facil de depurar mientras el streaming todavia es nuevo.
**Descartado:** paralelismo desde el principio.
**Me obliga a:** volver sobre esto en la semana 10, que es justo lo que se
hizo: `ThreadPoolExecutor` con `MAX_PARALELO`. Medido con 500 MB, la bajada
paso de 44.8 MB/s con un DataNode a 102.2 MB/s con cuatro.

## D10 - Hash consistente con 150 nodos virtuales

**Semana:** 10
**Decision:** `ConsistentHashPlacer`: anillo de hash con 150 vnodos por
nodo fisico, filtrando para que las replicas caigan en maquinas distintas.
**Por que:** con `hash % N`, agregar un DataNode remapea casi todas las
claves. Medido con 10000 claves, al pasar de 4 a 5 nodos el anillo movio el
**17.4%** (con modulo seria ~80%), y el reparto entre 4 nodos quedo en
22.1 / 27.0 / 26.2 / 24.7 por ciento. Sin vnodos ese reparto se
desbalancea por pura suerte de donde cae cada hash.
**Descartado:** `hash % N` y round robin (que fue la v1 de la semana 6).
**Me obliga a:** tener claro que **el hash es politica de colocacion
inicial, no mecanismo de busqueda**: el NameNode persiste donde quedo cada
bloque y no recalcula el hash al leer, asi puede desviarse del anillo si un
nodo esta lleno o caido sin que el sistema se pierda.

## D11 - Ubicaciones reconstruidas por block report

**Semana:** 10
**Decision:** el mapa `blockID -> DataNodes` no se persiste. Cada DataNode
manda su lista completa de bloques al arrancar y cada 60 s
(`BlockReport`), y avisa uno a uno con `BlockReceived`.
**Por que:** es lo que hace HDFS y lo que permite que D7 sea sostenible: lo
que hay que hacer durable es el namespace, no las ubicaciones. El que sabe
que bloques tiene un disco es el disco, no un registro que puede quedar
desactualizado.
**Descartado:** persistir las ubicaciones junto con el namespace.
**Me obliga a:** aceptar una ventana de hasta 60 s tras reiniciar el
NameNode en la que un `Open` puede responder `UNAVAILABLE` porque todavia
no sabe donde esta el bloque. Tambien da gratis la deteccion de bloques
huerfanos: los que ningun archivo reclama van a la cola de borrado.

## D12 - Separacion del `.proto` en cuatro archivos

**Semana:** 10
**Decision:** `dfsha_common.proto` con los tipos compartidos y uno por par
de comunicacion: `dfsha_namenode.proto`, `dfsha_datanode.proto` y
`dfsha_control.proto`.
**Por que:** cierra D4. Con los tres servicios ya implementados, el archivo
unico mezclaba tres contratos con ciclos de vida distintos. Separados, cada
par se documenta y evoluciona por su lado.
**Descartado:** seguir con un solo archivo.
**Me obliga a:** nada nuevo. Fue refactor puro, en su propio commit y con
las 98 pruebas pasando antes y despues.

## D13 - Replicacion por pipeline, store and forward

**Semana:** 11
**Decision:** factor de replica 3. El cliente sube cada bloque UNA vez al
primer DataNode; este lo guarda y lo reenvia al segundo, que lo reenvia
al tercero (`BlockHeader.pipeline`). La respuesta dice donde quedo de
verdad (`PutBlockResponse.replicas`).
**Por que:** con escritura paralela (el cliente manda a las tres) el
upstream del cliente se triplica, y el cliente suele ser el enlace mas
lento. Store and forward (guardar completo y despues reenviar) en vez de
reenviar chunk a chunk: es mucho menos codigo y el bloque que se reenvia
ya esta verificado en disco.
**Descartado:** `ParallelReplicator` y el pipeline por chunks de HDFS.
**Me obliga a:** aceptar mas latencia de escritura: medido en una
maquina, la subida con r=3 va a ~85 MB/s contra ~220 MB/s con r=1. Si un
nodo del pipeline falla se salta al siguiente: se pierde una copia, no la
escritura, y la re-replicacion la repone.

## D14 - Raft con biblioteca para los NameNodes, y solo el lider atiende

**Semana:** 12
**Decision:** tres NameNodes con Raft (`pysyncobj`), detras de la costura
`MetadataStore` (`LocalStore` / `RaftStore`). Toda escritura del
namespace es una operacion del log replicado. Solo el lider atiende al
cliente, incluidas las lecturas; los seguidores responden `UNAVAILABLE` y
el cliente busca al lider con `Ping`. Un lider que pasa 3 s sin oir a la
mayoria se retira (`leaderFallbackTimeout`).
**Por que:** D1 obligaba a replicar el NameNode (RNF2). Raft da un orden
unico de operaciones en todos los nodos (RNF3) sin implementarlo a mano,
que es donde estan los errores sutiles. Leer solo del lider evita
devolver un namespace atrasado. El journal en disco paga la deuda de D7:
un NameNode reiniciado recupera el namespace.
**Descartado:** primario-respaldo con copia del log (sin eleccion
automatica), y leer de los seguidores.
**Me obliga a:** que la maquina de estados sea determinista: los uuid, la
hora y la colocacion los decide el lider y viajan como argumentos. Y a
aceptar que con 2 de 3 NameNodes caidos el sistema deja de escribir:
consistencia sobre disponibilidad, que para metadatos es lo correcto.
Medido: un lider nuevo atiende escrituras entre 1.1 y 3.2 s despues de
matar al anterior.

## D15 - Las ubicaciones son estado blando; los DataNodes laten a todos

**Semana:** 12
**Decision:** el log de Raft lleva el namespace, los leases y por cada
bloque su indice, tamano, checksum y destinos iniciales. Quien tiene cada
bloque de verdad lo saben los heartbeats y los block reports, que cada
DataNode manda a TODOS los NameNodes, cada uno en su propio hilo. Solo el
lider manda comandos piggyback y agenda borrados.
**Por que:** es D11 llevado a varios NameNodes. Un lider recien elegido
ya sabe que DataNodes estan vivos y que bloques tienen, sin esperar un
minuto. Que solo el lider decida borrados evita que un seguidor atrasado
crea huerfano un bloque recien creado.
**Descartado:** latir solo al lider (el nuevo lider arrancaria a ciegas)
y replicar las ubicaciones por Raft (escribiria al log en cada latido).
**Me obliga a:** un hilo de latido por NameNode. Con un solo bucle, un
NameNode caido atrasaba el latido hacia el lider: en Windows una conexion
rechazada tarda 2.3 s en fallar (medido), y con latidos cada 3 s y
timeout de 6 s el lider declaraba muerto a un DataNode sano.

## D16 - Leases con vencimiento, y AddBlock para escribir sin tamano

**Semana:** 12
**Decision:** cada lease vence a los 300 s (`LEASE_TTL`) salvo que el
cliente lo renueve (`Renew`, cada TTL/3 mientras sube). El lider expira
los vencidos en su ciclo de mantenimiento: borra el archivo en
construccion y manda borrar sus bloques. `AddBlock` agrega un bloque a un
archivo en construccion sin conocer el tamano final.
**Por que:** paga la deuda de D8: un cliente que muere entre `Create` y
`Complete` ya no deja el path preso hasta un `rm` manual. `AddBlock` es
lo que permite `write()` en el SDK (RF3), igual que el `addBlock` de HDFS.
**Descartado:** sin vencimiento (D8) y un `abort` administrativo manual.
**Me obliga a:** que la hora la ponga el lider y viaje en la operacion
(determinismo de D14).

## D17 - Permisos estilo Unix, privados por defecto

**Semana:** 11
**Decision:** cada nodo tiene dueno, grupo y modo rwx. Crear o borrar
exige escribir en el padre; leer exige R; recorrer exige X en cada
ancestro. Lo nuevo nace `750` (directorios) o `640` (archivos) con el
grupo principal del usuario, que se llama como el usuario: privado hasta
que el dueno haga `chmod` con un grupo compartido. Todos los directorios
se comportan como sticky: cada uno borra solo lo suyo, salvo el dueno del
directorio. La raiz es `1777`, como `/tmp`.
**Por que:** el enunciado pide que "cada cliente gestione solo sus propios
archivos" y control de acceso "por usuario o grupo". El modelo de Unix es
conocido, cabe en tres campos y cubre los dos.
**Descartado:** ACL arbitrarias por entrada (mas expresivas, mas codigo y
mas dificiles de explicar) y un directorio raiz por usuario sin grupos.
**Me obliga a:** grupos en `usuarios.json` y a que `chmod` solo cambie a
un grupo del que el usuario es miembro.

## D18 - Tokens de bloque validados por el DataNode, sin preguntar

**Semana:** 12
**Decision:** el NameNode firma por cada bloque un token
`HMAC(secreto, op:block_id:vence)`, con `op` = `r` o `w`, y lo entrega en
`Create`, `AddBlock` y `Open`. El DataNode lo valida con el secreto
compartido antes de leer, escribir, borrar o replicar.
**Por que:** sin esto, la ACL del namespace no protege nada en el plano de
datos: quien adivine un `block_id` lo lee. Validar sin consultar al
NameNode mantiene a este fuera del camino de los datos (RNF1).
**Descartado:** que el DataNode le pregunte al NameNode por cada bloque.
**Me obliga a:** distribuir `DFSHA_BLOCK_SECRET` a NameNodes y DataNodes.

## D19 - Cifrado en el cliente, AES-256-GCM por chunk

**Semana:** 12
**Decision:** el cliente cifra cada chunk de 1 MB por separado con
AES-GCM; cada uno viaja y se guarda como nonce + cifrado + tag (28 bytes
mas). El AAD es `block_id:indice`. La clave es del usuario
(`~/.dfsha_key` o `DFSHA_KEY`). El sha256 que verifican el DataNode y el
NameNode es el del texto cifrado; la integridad del claro la da el tag.
**Por que:** cifrar en el cliente cubre a la vez el almacenamiento y el
transito hacia el DataNode, y ni un DataNode comprometido ni su disco
robado revelan nada. Por chunk para no tener nunca un bloque entero en
memoria. El AAD impide mover un chunk a otro bloque u otra posicion.
**Descartado:** cifrado en el DataNode (el DataNode veria texto claro).
**Me obliga a:** que quien no tenga la clave no pueda leer sus propios
archivos desde otra maquina: hay que llevarsela. Y el cliente se vuelve
el cuello de botella de CPU: en una maquina, 1 y 4 DataNodes dan casi el
mismo throughput (ver resultados).

## D20 - mTLS con una CA propia, activable por variable

**Semana:** 12
**Decision:** todo canal y todo puerto gRPC se abre por
`src/common/seguridad.py`. Con `DFSHA_TLS=1` las dos puntas presentan
certificado firmado por la CA del cluster (`scripts/gen_certs.py`).
Todos los certificados llevan el nombre `dfsha`. El trafico de Raft entre
NameNodes se cifra con `RAFT_PASSWORD`.
**Por que:** RNF6 pide cifrar las comunicaciones y "seguridad entre nodos
de datos y control". Mutuo para que un DataNode impostor no pueda latir
ni recibir bloques. Con una CA propia porque lo que se autentica son
nodos por IP privada, no un dominio publico (ver docs/despliegue.md). El
nombre fijo evita emitir un certificado por cada direccion posible.
**Descartado:** Let's Encrypt (el taller de nginx), TLS solo de servidor.
**Me obliga a:** copiar `ca.crt` y el par del rol a cada maquina, y a no
sacar `ca.key` de la maquina donde se genera.

---

## Plantilla para las siguientes

## Dn - Titulo

**Semana:**
**Decision:**
**Por que:**
**Descartado:**
**Me obliga a:**
