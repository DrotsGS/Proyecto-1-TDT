"""
Las cinco costuras del sistema.

Cada una arranco con una implementacion deliberadamente tonta.
Cuando quieras mejorar algo, cambias UNA clase de aqui y nada mas.

| Costura        | v1 (semana 6)     | Hoy                                  |
|----------------|-------------------|--------------------------------------|
| BlockPlacer    | RoundRobinPlacer  | ConsistentHashPlacer (S10)           |
| MetadataStore  | en memoria        | LocalStore / RaftStore (S12)         |
| Replicator     | copia unica       | PipelineReplicator (S11)             |
| AuthProvider   | NoopAuth          | FileAuth + BlockTokens (S7, S12)     |
| BlockCipher    | NoopCipher        | AesGcmCipher (S12)                   |
"""

from abc import ABC, abstractmethod
import base64
import bisect
import hashlib
import hmac
import itertools
import json
import os
import time

# Raiz del repositorio (este archivo esta en src/common/).
_RAIZ = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 128 MB es el valor de DISENO. Mientras desarrollas conviene bajarlo:
# con 128 MB, probar que el ultimo bloque parcial funciona obliga a
# generar archivos de 384 MB y cada iteracion se vuelve insoportable.
#
#   set DFSHA_BLOCK_SIZE=8388608        (8 MB, desarrollo)
#   set DFSHA_BLOCK_SIZE=1048576        (1 MB, pruebas automaticas)
#
# Va por variable de entorno y no editando esta linea para que las
# pruebas puedan usar bloques chicos sin tocar el codigo, que es el
# mismo patron de config.py.
BLOCK_SIZE = int(os.environ.get("DFSHA_BLOCK_SIZE", 128 * 1024 * 1024))

# Semana 11: tres copias de cada bloque, en maquinas distintas. Si hay
# menos DataNodes vivos, se ponen las que quepan y la re-replicacion
# completa el resto cuando aparezcan mas.
REPLICATION_FACTOR = int(os.environ.get("DFSHA_REPLICATION", "3"))
CHUNK_SIZE = 1024 * 1024    # tamano de cada mensaje del stream


# ---------------------------------------------------------------------
# 1. Colocacion de bloques    semanas 10-11
# ---------------------------------------------------------------------
class BlockPlacer(ABC):
    @abstractmethod
    def place(self, block_id: str, datanodes: list, n: int) -> list:
        """Devuelve las n replicas donde va este bloque."""


class RoundRobinPlacer(BlockPlacer):
    """v1. Simple, pero se desbalancea si entra o sale un nodo."""

    def __init__(self):
        self._counter = itertools.count()

    def place(self, block_id, datanodes, n):
        if not datanodes:
            return []
        start = next(self._counter) % len(datanodes)
        k = min(n, len(datanodes))
        return [datanodes[(start + i) % len(datanodes)] for i in range(k)]


class ConsistentHashPlacer(BlockPlacer):
    """Anillo de hash consistente con nodos virtuales.

    Por que no hash % N: con modulo, agregar un DataNode remapea casi
    todas las claves (de hash % 4 a hash % 5 practicamente ningun bloque
    cae donde estaba). Con un anillo solo se mueve alrededor de 1/N.

    Cada nodo fisico se replica VNODOS veces en el anillo. Sin nodos
    virtuales, con pocos nodos el reparto queda muy desbalanceado: uno
    puede quedarse con el 60% de las claves por pura suerte de donde
    cayo su hash.

    OJO: esto es politica de colocacion INICIAL, no mecanismo de
    busqueda. El NameNode persiste donde quedo cada bloque y no
    recalcula el hash al leer; por eso puede desviarse del anillo si un
    nodo esta lleno o caido sin que el sistema se pierda.
    """

    VNODOS = 150

    def __init__(self, nodos=None):
        self._anillo = {}       # posicion -> nodo fisico
        self._posiciones = []   # ordenado, para bisect
        for n in (nodos or []):
            self.agregar(n)

    @staticmethod
    def _hash(clave):
        return int(hashlib.md5(clave.encode()).hexdigest()[:8], 16)

    def agregar(self, nodo):
        for i in range(self.VNODOS):
            self._anillo[self._hash("{}#{}".format(nodo, i))] = nodo
        self._posiciones = sorted(self._anillo.keys())

    def quitar(self, nodo):
        for i in range(self.VNODOS):
            self._anillo.pop(self._hash("{}#{}".format(nodo, i)), None)
        self._posiciones = sorted(self._anillo.keys())

    def _sincronizar(self, vivos):
        for nodo in vivos - set(self._anillo.values()):
            self.agregar(nodo)
        for nodo in set(self._anillo.values()) - vivos:
            self.quitar(nodo)

    def place(self, block_id, datanodes, n):
        """Los n primeros nodos FISICOS distintos, en sentido horario."""
        vivos = set(datanodes)
        self._sincronizar(vivos)
        if not self._posiciones:
            return []

        inicio = bisect.bisect_left(self._posiciones, self._hash(block_id))
        salida = []
        total = len(self._posiciones)
        cuantos = min(n, len(vivos))
        for k in range(total):
            nodo = self._anillo[self._posiciones[(inicio + k) % total]]
            # Caminando el anillo te topas varias veces con el mismo nodo
            # fisico (son sus vnodos). Si no se filtra, las 3 "replicas"
            # pueden caer en la misma maquina, que es justo lo contrario
            # de replicar.
            if nodo not in salida:
                salida.append(nodo)
            if len(salida) == cuantos:
                break
        return salida


# ---------------------------------------------------------------------
# 2. Almacen de metadatos     semanas 8 y 12
# ---------------------------------------------------------------------
class MetadataStore(ABC):
    """Aplica operaciones sobre el estado del NameNode.

    Toda escritura del namespace pasa por ejecutar(): con LocalStore se
    aplica directo; con RaftStore primero se replica en la mayoria de
    NameNodes y solo despues se aplica, en el mismo orden en todos.
    Las implementaciones viven en namenode/estado.py.
    """

    @abstractmethod
    def ejecutar(self, op: str, *args): ...

    @abstractmethod
    def es_lider(self) -> bool: ...

    @abstractmethod
    def lider(self) -> str: ...


# ---------------------------------------------------------------------
# 3. Replicacion              semanas 11-12
# ---------------------------------------------------------------------
class Replicator(ABC):
    @abstractmethod
    def intentos(self, targets: list) -> list:
        """Formas de escribir el bloque, en orden de preferencia.

        Cada intento es (head, pipeline): el cliente manda el bloque al
        head, y el head lo reenvia a lo largo del pipeline.
        """


class SingleWriteReplicator(Replicator):
    """v1. Una sola copia: prueba cada destino hasta que uno acepte."""

    def intentos(self, targets):
        return [(t, []) for t in targets]


class PipelineReplicator(Replicator):
    """Semana 11. cliente -> DN1 -> DN2 -> DN3.

    El cliente sube el bloque UNA vez; cada DataNode lo reenvia al
    siguiente. Asi el upstream del cliente no se triplica, que con
    ParallelReplicator (el cliente escribe a las tres) si pasaria.

    Si el head no responde, el siguiente pasa a ser el head con el
    resto del pipeline: se pierde una replica, no la escritura. La
    re-replicacion del NameNode completa la que falte.
    """

    def intentos(self, targets):
        return [(targets[i], list(targets[i + 1:]))
                for i in range(len(targets))]


# ---------------------------------------------------------------------
# 4. Autenticacion            semanas 7, 9 y 12
# ---------------------------------------------------------------------
class AuthProvider(ABC):
    @abstractmethod
    def login(self, user: str, password: str) -> str: ...

    @abstractmethod
    def verify(self, token: str):
        """Devuelve el usuario si el token es valido, None si no."""

    def grupos(self, user: str) -> list:
        """Grupos del usuario, para las ACL. El primero es el principal."""
        return [user]


class NoopAuth(AuthProvider):
    """v1. Todo el mundo pasa. Semana 6."""

    def login(self, user, password):
        return "fake-token:" + user

    def verify(self, token):
        if token.startswith("fake-token:"):
            return token.split(":", 1)[1]
        return None


class FileAuth(AuthProvider):
    """Usuarios en un JSON, tokens firmados con HMAC-SHA256.

    No es JWT, pero tiene lo esencial: la contrasena solo viaja en el
    login (y dentro de TLS), y el token expira.

    Formato del token: base64url("<usuario>:<vencimiento>:<firma>").

    Cada usuario en el JSON puede ser solo el sha256 del password, o
    un objeto con sus grupos:

        {"drots": "<sha256>",
         "ana":   {"password": "<sha256>", "groups": ["equipo"]}}
    """

    def __init__(self, ruta_usuarios, secreto, ttl=3600):
        with open(ruta_usuarios, encoding="utf-8") as f:
            crudo = json.load(f)
        self.usuarios = {}
        self._grupos = {}
        for user, valor in crudo.items():
            if isinstance(valor, dict):
                self.usuarios[user] = valor.get("password", "")
                extra = [g for g in valor.get("groups", []) if g != user]
            else:
                self.usuarios[user] = valor
                extra = []
            # El grupo principal se llama como el usuario, igual que en
            # la mayoria de distribuciones Linux.
            self._grupos[user] = [user] + extra
        self.secreto = secreto.encode()
        self.ttl = ttl

    def _firmar(self, payload):
        return hmac.new(self.secreto, payload.encode(),
                        hashlib.sha256).hexdigest()[:32]

    def login(self, user, password):
        h = hashlib.sha256(password.encode()).hexdigest()
        # compare_digest tambien aqui: no filtres si el usuario existe
        # por el tiempo que tarda en responder.
        if not hmac.compare_digest(self.usuarios.get(user, ""), h):
            return None
        payload = "{}:{}".format(user, int(time.time()) + self.ttl)
        return base64.urlsafe_b64encode(
            "{}:{}".format(payload, self._firmar(payload)).encode()).decode()

    def verify(self, token):
        try:
            crudo = base64.urlsafe_b64decode(token.encode()).decode()
            user, exp, firma = crudo.rsplit(":", 2)
            # compare_digest en vez de ==: la comparacion en tiempo
            # constante evita que se pueda adivinar la firma byte a byte
            # midiendo cuanto tarda el rechazo.
            if not hmac.compare_digest(firma, self._firmar(
                    "{}:{}".format(user, exp))):
                return None
            if int(exp) < time.time():
                return None
            return user
        except Exception:
            return None

    def grupos(self, user):
        return list(self._grupos.get(user, [user]))


SECRETO_DEV = "dfsha-dev-no-usar-en-produccion"


def build_auth():
    """Escoge el proveedor segun el entorno. Es la costura en accion.

    DFSHA_USERS   ruta del JSON de usuarios (por defecto deploy/usuarios.json)
    DFSHA_SECRET  secreto del HMAC. Nunca va escrito en el codigo.

    Si no hay archivo de usuarios se cae a NoopAuth, para que un clon
    recien hecho arranque sin configurar nada.
    """
    ruta = os.environ.get("DFSHA_USERS") or os.path.join(
        _RAIZ, "deploy", "usuarios.json")
    if not os.path.exists(ruta):
        print("[auth] sin {}: usando NoopAuth (solo desarrollo)".format(ruta))
        return NoopAuth()
    secreto = os.environ.get("DFSHA_SECRET", SECRETO_DEV)
    if secreto == SECRETO_DEV:
        print("[auth] DFSHA_SECRET sin definir: usando el secreto de desarrollo")
    print("[auth] FileAuth con los usuarios de {}".format(ruta))
    return FileAuth(ruta, secreto)


class BlockTokens:
    """Block access tokens: el NameNode firma, el DataNode verifica.

    Semana 12. Sin esto, cualquiera que adivine un block_id y llegue al
    puerto de un DataNode puede leerlo o pisarlo: la ACL del namespace
    no serviria de nada en el plano de datos.

    El token es HMAC(secreto, "<op>:<block_id>:<vence>"). El DataNode
    lo valida con el secreto compartido, SIN consultar al NameNode: si
    tuviera que preguntarle por cada bloque, el NameNode volveria a ser
    cuello de botella y se caeria RNF1.

    op es "r" (leer) o "w" (escribir, replicar o borrar).
    """

    def __init__(self, secreto, ttl=3600):
        self.secreto = secreto.encode()
        self.ttl = ttl

    def _firma(self, op, block_id, vence):
        msg = "{}:{}:{}".format(op, block_id, vence).encode()
        return hmac.new(self.secreto, msg, hashlib.sha256).hexdigest()[:32]

    def emitir(self, op, block_id, ahora=None):
        vence = int((ahora or time.time()) + self.ttl)
        return "{}:{}:{}".format(op, vence, self._firma(op, block_id, vence))

    def verificar(self, token, op, block_id, ahora=None):
        try:
            t_op, vence, firma = token.split(":", 2)
            if t_op != op:
                return False
            if int(vence) < (ahora or time.time()):
                return False
            return hmac.compare_digest(firma, self._firma(op, block_id, vence))
        except (ValueError, AttributeError):
            return False


def build_block_tokens():
    secreto = os.environ.get("DFSHA_BLOCK_SECRET", SECRETO_DEV + "-bloques")
    return BlockTokens(secreto)


# ---------------------------------------------------------------------
# 5. Cifrado de bloques       semana 12
# ---------------------------------------------------------------------
class BlockCipher(ABC):
    """Cifra cada chunk por separado.

    Por chunk y no por bloque: asi ni el cliente ni el DataNode tienen
    que tener un bloque de 128 MB entero en memoria. Cada chunk cifrado
    pesa OVERHEAD bytes mas que el original.
    """

    OVERHEAD = 0

    @abstractmethod
    def encrypt(self, data: bytes, aad: bytes = b"") -> bytes: ...

    @abstractmethod
    def decrypt(self, data: bytes, aad: bytes = b"") -> bytes: ...

    def tam_cifrado(self, size, chunk=None):
        """Bytes que ocupa en el DataNode un bloque de size bytes claros."""
        chunk = chunk or CHUNK_SIZE
        n = (size + chunk - 1) // chunk
        return size + n * self.OVERHEAD


class NoopCipher(BlockCipher):
    """v1. Passthrough."""

    def encrypt(self, data, aad=b""):
        return data

    def decrypt(self, data, aad=b""):
        return data


class AesGcmCipher(BlockCipher):
    """AES-256-GCM en el cliente. Los DataNodes nunca ven texto claro.

    Cifrar en el cliente cubre a la vez el cifrado en reposo y en
    transito hacia el DataNode, y es lo mas fuerte: ni un DataNode
    comprometido ni su disco robado revelan nada.

    Cada chunk se guarda como nonce(12) + texto cifrado + tag(16). El
    aad es "<block_id>:<indice del chunk>": GCM lo autentica, asi que
    un chunk movido a otro bloque u otra posicion falla al descifrar.

    El nonce sale de HMAC(clave, aad) y no al azar. Asi un reintento de
    subida produce EXACTAMENTE los mismos bytes: si el primer intento
    alcanzo a quedar en un DataNode pero su respuesta se perdio, esa
    copia y la del reintento son identicas (con nonce aleatorio, las
    replicas de un mismo bloque quedaban distintas). Es seguro porque
    cada block_id es unico y lo que contiene no cambia nunca (WORM): un
    mismo nonce nunca cifra dos textos distintos.
    """

    OVERHEAD = 12 + 16

    def __init__(self, clave):
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        if len(clave) != 32:
            raise ValueError("la clave de AES-256 debe tener 32 bytes")
        self.aes = AESGCM(clave)
        self._clave_nonce = hmac.new(clave, b"dfsha-nonce", hashlib.sha256).digest()

    def encrypt(self, data, aad=b""):
        if aad:
            nonce = hmac.new(self._clave_nonce, aad, hashlib.sha256).digest()[:12]
        else:
            nonce = os.urandom(12)
        return nonce + self.aes.encrypt(nonce, data, aad or None)

    def decrypt(self, data, aad=b""):
        return self.aes.decrypt(data[:12], data[12:], aad or None)


def key_file():
    return os.environ.get(
        "DFSHA_KEY_FILE", os.path.join(os.path.expanduser("~"), ".dfsha_key"))


def build_cipher():
    """AES-GCM por defecto. DFSHA_CIPHER=none lo apaga.

    La clave sale de DFSHA_KEY (64 hex) o del archivo ~/.dfsha_key, que
    se crea la primera vez. Es la clave del USUARIO: quien no la tenga
    no puede leer sus archivos aunque tenga acceso a los DataNodes.
    Para bajar un archivo desde otra maquina hay que llevarse la clave.
    """
    if os.environ.get("DFSHA_CIPHER", "aes").lower() == "none":
        return NoopCipher()
    hexa = os.environ.get("DFSHA_KEY")
    if not hexa:
        ruta = key_file()
        if not os.path.exists(ruta):
            # Atomico: se escribe aparte y se renombra. Si no, otro proceso
            # (o hilo) podria leer el archivo recien creado pero vacio. Y
            # si dos la crean a la vez, gana la primera: replace no pisa
            # nada que otro ya haya dejado listo.
            temporal = "{}.{}.tmp".format(ruta, os.getpid())
            with open(temporal, "w", encoding="utf-8") as f:
                f.write(os.urandom(32).hex())
            try:
                os.chmod(temporal, 0o600)
            except OSError:
                pass
            if os.path.exists(ruta):
                os.remove(temporal)
            else:
                os.replace(temporal, ruta)
                print("[cifrado] clave nueva en {} (guardala: sin ella no se "
                      "pueden leer tus archivos)".format(ruta))
        with open(ruta, encoding="utf-8") as f:
            hexa = f.read().strip()
    return AesGcmCipher(bytes.fromhex(hexa))


# ---------------------------------------------------------------------
# utilidades
# ---------------------------------------------------------------------
def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def num_blocks(size: int, block_size: int = BLOCK_SIZE) -> int:
    return max(1, (size + block_size - 1) // block_size)
