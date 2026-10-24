"""Namespace en memoria.

Semana 7 lo terminas. Semana 12 lo pones detras de Raft sin cambiar
esta interfaz: el namespace no sabe que esta replicado, solo que las
operaciones le llegan en orden (ver namenode/estado.py).

Las operaciones de borrado devuelven TRES valores: (ok, mensaje, huerfanos).
La tercera es la lista de block_id que quedaron sin dueno; el NameNode
los manda borrar por piggyback en la respuesta al heartbeat.

Permisos (semana 11, RNF6): cada nodo tiene dueno, grupo y modo rwx al
estilo Unix. Ver puede() y acceso().

Todo lo que cambia el arbol tiene que ser DETERMINISTA: Raft aplica la
misma operacion en cada NameNode y todos deben quedar iguales. Por eso
los tiempos y los identificadores llegan como argumentos y nunca se
generan aqui dentro.
"""

import time
from dataclasses import dataclass, field

UNDER_CONSTRUCTION = 0
COMMITTED = 1

R, W, X = 4, 2, 1
STICKY = 0o1000

# Privado por defecto: el grupo principal de cada usuario es el mismo
# usuario, asi que 750/640 solo dejan entrar al dueno. Para compartir
# basta con un chmod que cambie el grupo a uno compartido.
MODO_DIR = 0o750
MODO_ARCHIVO = 0o640
# La raiz es como /tmp: todos crean, nadie borra lo ajeno.
MODO_RAIZ = 0o1777


@dataclass
class Node:
    name: str
    is_dir: bool
    size: int = 0
    state: int = COMMITTED
    created_at: float = field(default_factory=time.time)
    children: dict = field(default_factory=dict)   # solo si is_dir
    blocks: list = field(default_factory=list)     # solo si es archivo
    lease_id: str = ""                             # solo si UNDER_CONSTRUCTION
    lease_vence: float = 0                         # 0 = no expira
    owner: str = ""
    group: str = ""
    mode: int = 0o777


class Namespace:
    def __init__(self):
        self.root = Node(name="", is_dir=True, owner="root", group="root",
                         mode=MODO_RAIZ, created_at=0)

    @staticmethod
    def _split(path):
        return [p for p in path.replace("\\", "/").strip("/").split("/") if p]

    def resolve(self, path):
        node = self.root
        for part in self._split(path):
            if not node.is_dir or part not in node.children:
                return None
            node = node.children[part]
        return node

    def parent_of(self, path):
        parts = self._split(path)
        if not parts:
            return None, ""
        return self.resolve("/".join(parts[:-1])), parts[-1]

    # ---------------- permisos ----------------

    @staticmethod
    def puede(nodo, user, grupos, bits):
        """Tiene user los bits pedidos (R, W, X) sobre nodo?

        Igual que Unix: si es el dueno cuentan los bits del dueno; si no,
        y comparte grupo, los del grupo; si no, los de otros. Solo una
        clase aplica, no se suman.
        """
        if user == nodo.owner:
            clase = (nodo.mode >> 6) & 7
        elif nodo.group in grupos:
            clase = (nodo.mode >> 3) & 7
        else:
            clase = nodo.mode & 7
        return clase & bits == bits

    def acceso(self, path, user, grupos, bits):
        """Revisa X en cada ancestro y bits sobre el destino.

        Devuelve (ok, mensaje). Si el destino no existe responde ok: que
        no exista lo decide la operacion, con su propio codigo.
        """
        nodo = self.root
        for part in self._split(path):
            if not nodo.is_dir or part not in nodo.children:
                return True, "ok"
            if not self.puede(nodo, user, grupos, X):
                return False, "permiso denegado"
            nodo = nodo.children[part]
        if bits and not self.puede(nodo, user, grupos, bits):
            return False, "permiso denegado"
        return True, "ok"

    def acceso_padre(self, path, user, grupos, bits=W | X):
        """Para crear o borrar hace falta escribir en el directorio padre."""
        parts = self._split(path)
        if not parts:
            return True, "ok"
        return self.acceso("/".join(parts[:-1]), user, grupos, bits)

    def puede_borrar(self, path, user):
        """Sticky en todos los directorios: cada uno borra solo lo suyo.

        Es lo que pide el enunciado: "cada cliente gestiona solo sus
        propios archivos". Escribir en un directorio compartido no da
        derecho a borrar lo que subio otro.
        """
        nodo = self.resolve(path)
        padre, _ = self.parent_of(path)
        if nodo is None or padre is None:
            return True
        return user in (nodo.owner, padre.owner)

    def chmod(self, path, user, mode, group=""):
        nodo = self.resolve(path)
        if nodo is None:
            return False, "no existe"
        if user != nodo.owner:
            return False, "permiso denegado"
        nodo.mode = mode & 0o7777
        if group:
            nodo.group = group
        return True, "ok"

    # ---------------- RF1 ----------------

    def mkdir(self, path, owner="", group="", mode=MODO_DIR, ahora=None):
        parent, name = self.parent_of(path)
        if parent is None or not parent.is_dir:
            return False, "el directorio padre no existe"
        if name in parent.children:
            return False, "ya existe"
        parent.children[name] = Node(
            name=name, is_dir=True, owner=owner, group=group or owner,
            mode=mode, created_at=ahora if ahora is not None else time.time())
        return True, "ok"

    def ls(self, path):
        node = self.resolve(path)
        if node is None or not node.is_dir:
            return None
        # Los archivos UNDER_CONSTRUCTION no se listan. Eso es el WORM:
        # un archivo o no existe, o existe completo.
        return [c for c in node.children.values()
                if c.is_dir or c.state == COMMITTED]

    def stat(self, path):
        """Devuelve el Node o None si no existe.

        A diferencia de ls, stat SI ve los archivos en construccion: es la
        unica forma de diagnosticar una subida que quedo a medias.
        """
        return self.resolve(path)

    def rmdir(self, path, recursivo=False):
        """Borra un directorio. D5: por defecto exige que este vacio.

        El parametro recursivo ya esta en la firma aunque el protocolo
        todavia no tenga como pedirlo (PathRequest no lleva el flag).
        Cuando se agregue al .proto, la logica no cambia.
        """
        nodo = self.resolve(path)
        if nodo is None:
            return False, "no existe", []
        if not nodo.is_dir:
            return False, "no es un directorio", []
        if nodo is self.root:
            return False, "no se puede borrar la raiz", []
        if nodo.children and not recursivo:
            return False, "el directorio no esta vacio", []

        huerfanos = self._recolectar_bloques(nodo)
        padre, nombre = self.parent_of(path)
        del padre.children[nombre]
        return True, "ok", huerfanos

    def rm(self, path):
        """Borra un archivo. D6 opcion (b): el rm siempre gana.

        Si el archivo estaba UNDER_CONSTRUCTION, el nodo desaparece y con
        el su lease. El cliente que estuviera subiendo se entera cuando su
        Complete falle con FAILED_PRECONDITION. Los bloques ya escritos
        quedan huerfanos y se agendan para borrado.

        Un path no puede quedar bloqueado para siempre por un cliente que
        se murio, y eso importa mas que unos bloques huerfanos temporales.
        """
        nodo = self.resolve(path)
        if nodo is None:
            return False, "no existe", []
        if nodo.is_dir:
            return False, "es un directorio, usa rmdir", []

        huerfanos = list(nodo.blocks)
        en_construccion = nodo.state == UNDER_CONSTRUCTION
        padre, nombre = self.parent_of(path)
        del padre.children[nombre]
        msg = "ok (se cancelo una subida en curso)" if en_construccion else "ok"
        return True, msg, huerfanos

    # ---------------- RF2: ciclo de vida de escritura (WORM) ----------------

    def create(self, path, size, block_ids, lease_id, owner="", group="",
               mode=MODO_ARCHIVO, vence=0, ahora=None):
        """Crea el archivo UNDER_CONSTRUCTION: existe, pero es invisible.

        Invisible para ls, no para stat. Solo el Complete lo hace visible,
        y desde ahi es inmutable: eso es el WORM.
        """
        padre, nombre = self.parent_of(path)
        if padre is None or not padre.is_dir:
            return False, "el directorio padre no existe"
        if nombre in padre.children:
            return False, "ya existe"
        padre.children[nombre] = Node(
            name=nombre, is_dir=False, size=size,
            state=UNDER_CONSTRUCTION, blocks=list(block_ids),
            lease_id=lease_id, lease_vence=vence,
            owner=owner, group=group or owner, mode=mode,
            created_at=ahora if ahora is not None else time.time())
        return True, "ok"

    def _en_construccion(self, path, lease_id):
        nodo = self.resolve(path)
        if (nodo is None or nodo.is_dir or nodo.state != UNDER_CONSTRUCTION
                or nodo.lease_id != lease_id):
            return None
        return nodo

    def add_block(self, path, lease_id, block_id, size):
        """Un bloque mas al final de un archivo en construccion.

        Es lo que permite el write() del SDK: se escribe sin saber el
        tamano final, bloque a bloque, igual que el addBlock de HDFS.
        """
        nodo = self._en_construccion(path, lease_id)
        if nodo is None:
            return False, "el lease ya no es valido"
        nodo.blocks.append(block_id)
        nodo.size += size
        return True, "ok"

    def renew(self, path, lease_id, vence):
        nodo = self._en_construccion(path, lease_id)
        if nodo is None:
            return False, "el lease ya no es valido"
        nodo.lease_vence = vence
        return True, "ok"

    def complete(self, path, lease_id):
        """El commit. Un archivo o no existe, o existe completo."""
        nodo = self.resolve(path)
        if nodo is None:
            # D6: puede que le hayan hecho rm mientras subia.
            return False, "el lease ya no es valido"
        if nodo.is_dir:
            return False, "es un directorio, usa rmdir"
        if nodo.state == COMMITTED:
            return False, "el archivo ya esta completo"
        if nodo.lease_id != lease_id:
            return False, "el lease ya no es valido"
        nodo.state = COMMITTED
        nodo.lease_id = ""
        nodo.lease_vence = 0
        return True, "ok"

    def abort(self, path, lease_id):
        """Cancela una subida. Los bloques ya escritos quedan huerfanos."""
        nodo = self._en_construccion(path, lease_id)
        if nodo is None:
            return False, "el lease ya no es valido", []
        huerfanos = list(nodo.blocks)
        padre, nombre = self.parent_of(path)
        del padre.children[nombre]
        return True, "ok", huerfanos

    def expirar(self, ahora):
        """Borra los archivos en construccion cuyo lease vencio.

        Paga la deuda de D8: un cliente que muere entre Create y Complete
        ya no deja el path preso hasta que alguien haga rm a mano.
        Devuelve [(path, huerfanos)].
        """
        vencidos = []

        def recorrer(nodo, ruta):
            for nombre, hijo in list(nodo.children.items()):
                p = ruta + "/" + nombre
                if hijo.is_dir:
                    recorrer(hijo, p)
                elif (hijo.state == UNDER_CONSTRUCTION and hijo.lease_vence
                      and hijo.lease_vence < ahora):
                    del nodo.children[nombre]
                    vencidos.append((p, list(hijo.blocks)))

        recorrer(self.root, "")
        return vencidos

    def _recolectar_bloques(self, nodo):
        """Todos los block_id del subarbol. Para el borrado diferido."""
        if not nodo.is_dir:
            return list(nodo.blocks)
        salida = []
        for hijo in nodo.children.values():
            salida.extend(self._recolectar_bloques(hijo))
        return salida
