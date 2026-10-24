"""El estado del NameNode y como se replica.

Semana 12, hito 3. Todo lo que el NameNode tiene que recordar despues
de caerse vive en Metadatos:

    ns         el arbol de directorios y archivos (namenode/namespace.py)
    block_map  block_id -> {index, size, sha256, datanodes}
    leases     lease_id -> path

Y TODA escritura pasa por MetadataStore.ejecutar(op, *args). Hay dos
implementaciones, y el resto del NameNode no sabe cual tiene:

    LocalStore   aplica la operacion directo. Un solo NameNode, sin
                 replicacion. Es lo que usan las pruebas y el modo
                 "tres terminales".
    RaftStore    la replica primero en la mayoria de NameNodes con Raft
                 (biblioteca pysyncobj) y despues la aplica, en el mismo
                 orden, en todos. Tambien la escribe a un journal en
                 disco: un NameNode que se reinicia recupera el namespace
                 entero. Eso paga la deuda de D7.

Lo que NO se replica a proposito: donde esta cada bloque (los
'datanodes' de block_map). Eso lo reconstruyen los BlockReport (D11),
y por eso los DataNodes laten hacia todos los NameNodes, no solo al
lider.

Regla de oro: Metadatos.aplicar() tiene que ser DETERMINISTA. Corre en
cada NameNode por separado y todos deben terminar iguales. Los uuid, la
hora y la colocacion de bloques los decide el lider ANTES y llegan como
argumentos.
"""

import os
import threading

from namenode.namespace import Namespace
from common.interfaces import MetadataStore


class Metadatos:
    def __init__(self):
        self.ns = Namespace()
        self.block_map = {}
        self.leases = {}
        self.lock = threading.RLock()

    # pysyncobj guarda el estado con pickle; un lock no se puede
    # serializar, asi que se excluye y se recrea al cargar.
    def __getstate__(self):
        d = dict(self.__dict__)
        d.pop("lock", None)
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.lock = threading.RLock()

    # ---------------- la maquina de estados ----------------

    def aplicar(self, op, args):
        with self.lock:
            return getattr(self, "_op_" + op)(*args)

    def _registrar_bloque(self, block_id, index, size, destinos):
        self.block_map[block_id] = {
            "index": index, "size": size,
            "datanodes": list(destinos), "sha256": ""}

    def _olvidar(self, huerfanos):
        for block_id in huerfanos:
            self.block_map.pop(block_id, None)

    def _purgar_leases(self):
        """Un borrado puede haberse llevado por delante un archivo en
        construccion (D6). Su lease deja de existir con el."""
        vigentes = {}
        for lease_id, path in self.leases.items():
            nodo = self.ns.stat(path)
            if nodo is not None and nodo.lease_id == lease_id:
                vigentes[lease_id] = path
        self.leases = vigentes

    def _op_mkdir(self, path, owner, group, mode, ahora):
        return self.ns.mkdir(path, owner, group, mode, ahora)

    def _op_chmod(self, path, user, mode, group):
        return self.ns.chmod(path, user, mode, group)

    def _op_rmdir(self, path):
        ok, msg, huerfanos = self.ns.rmdir(path)
        if ok:
            self._olvidar(huerfanos)
            self._purgar_leases()
        return ok, msg, huerfanos

    def _op_rm(self, path):
        ok, msg, huerfanos = self.ns.rm(path)
        if ok:
            self._olvidar(huerfanos)
            self._purgar_leases()
        return ok, msg, huerfanos

    def _op_create(self, path, size, owner, group, mode, lease_id, vence,
                   ahora, bloques):
        """bloques: [(block_id, index, size, [node_id, ...]), ...]"""
        ok, msg = self.ns.create(
            path, size, [b[0] for b in bloques], lease_id,
            owner, group, mode, vence, ahora)
        if ok:
            for b in bloques:
                self._registrar_bloque(*b)
            self.leases[lease_id] = path
        return ok, msg

    def _op_add_block(self, lease_id, bloque):
        path = self.leases.get(lease_id)
        if path is None:
            return False, "el lease ya no es valido"
        block_id, index, size, destinos = bloque
        ok, msg = self.ns.add_block(path, lease_id, block_id, size)
        if ok:
            self._registrar_bloque(block_id, index, size, destinos)
        return ok, msg

    def _op_renew(self, lease_id, vence):
        path = self.leases.get(lease_id)
        if path is None:
            return False, "el lease ya no es valido"
        return self.ns.renew(path, lease_id, vence)

    def _op_complete(self, path, lease_id, checksums):
        ok, msg = self.ns.complete(path, lease_id)
        if ok:
            for block_id, sha in checksums.items():
                if block_id in self.block_map:
                    self.block_map[block_id]["sha256"] = sha
            self.leases.pop(lease_id, None)
        return ok, msg

    def _op_abort(self, lease_id):
        path = self.leases.get(lease_id)
        if path is None:
            return False, "lease desconocido", [], ""
        ok, msg, huerfanos = self.ns.abort(path, lease_id)
        self.leases.pop(lease_id, None)
        if ok:
            self._olvidar(huerfanos)
        return ok, msg, huerfanos, path

    def _op_expirar(self, ahora):
        vencidos = self.ns.expirar(ahora)
        for _, huerfanos in vencidos:
            self._olvidar(huerfanos)
        if vencidos:
            self._purgar_leases()
        return vencidos


class LocalStore(MetadataStore):
    """Un NameNode solo. Aplica directo."""

    def __init__(self):
        self.meta = Metadatos()

    def ejecutar(self, op, *args):
        return self.meta.aplicar(op, args)

    def es_lider(self):
        return True

    def lider(self):
        return ""

    def estado(self):
        return {"modo": "local", "lider": True}


class NoSoyLider(Exception):
    """La operacion no se pudo replicar: no hay lider o no soy yo."""


class RaftStore(MetadataStore):
    """Varios NameNodes con Raft (pysyncobj).

    yo     "host:puerto" de Raft de este NameNode
    otros  ["host:puerto", ...] de Raft de los demas

    Con tres NameNodes, el cluster sigue aceptando escrituras con uno
    caido: Raft necesita mayoria (2 de 3). Con dos caidos, deja de
    escribir antes que arriesgar dos lideres a la vez: elige
    consistencia sobre disponibilidad, que para metadatos es lo correcto.
    """

    TIMEOUT = 5.0

    def __init__(self, yo, otros, carpeta=None, password=None):
        from pysyncobj import SyncObj, SyncObjConf, SyncObjConsumer, replicated

        class _Consumidor(SyncObjConsumer):
            def __init__(self):
                super().__init__()
                # Despues del super(): pysyncobj solo serializa los
                # atributos creados despues de su propio __init__.
                self.meta = Metadatos()

            @replicated
            def aplicar(self, op, args):
                return self.meta.aplicar(op, args)

        self._consumidor = _Consumidor()
        conf = {"dynamicMembershipChange": False,
                "commandsWaitLeader": True,
                "appendEntriesUseBatch": True,
                # Un lider que lleva 3 s sin oir a la mayoria se retira.
                # Sin esto (por defecto son 30 s), un lider aislado de la
                # red seguiria contestando lecturas con datos viejos
                # mientras los demas ya eligieron a otro.
                "leaderFallbackTimeout": 3.0}
        if carpeta:
            os.makedirs(carpeta, exist_ok=True)
            nombre = yo.replace(":", "_")
            conf["journalFile"] = os.path.join(carpeta, nombre + ".journal")
            conf["fullDumpFile"] = os.path.join(carpeta, nombre + ".dump")
        if password:
            # Cifra el trafico entre NameNodes (par ControlNode<->ControlNode).
            conf["password"] = password
        self._raft = SyncObj(yo, list(otros), SyncObjConf(**conf),
                             consumers=[self._consumidor])
        self.yo = yo

    @property
    def meta(self):
        # Siempre por la propiedad: al cargar el dump de disco, pysyncobj
        # REEMPLAZA el objeto, asi que guardar una referencia vieja seria
        # leer un estado que ya no existe.
        return self._consumidor.meta

    def ejecutar(self, op, *args):
        from pysyncobj import SyncObjException
        try:
            return self._consumidor.aplicar(
                op, args, sync=True, timeout=self.TIMEOUT)
        except SyncObjException as err:
            raise NoSoyLider("no se pudo replicar ({})".format(err)) from err

    def es_lider(self):
        return self._raft._isLeader()

    def lider(self):
        lider = self._raft._getLeader()
        return "" if lider is None else str(lider)

    def estado(self):
        s = self._raft.getStatus()
        return {"modo": "raft", "lider": self.es_lider(),
                "lider_addr": self.lider(), "term": s.get("raft_term"),
                "commit": s.get("commit_idx")}

    def cerrar(self):
        self._raft.destroy()
