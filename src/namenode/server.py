"""
NameNode de DFSha.

Semana 6:  Ping, Mkdir y Ls, y el registro de DataNodes por heartbeat.
Semana 7:  Rmdir, Rm y Stat, con el mapeo uniforme a StatusCode.
Semana 8:  Create, Complete, Abort y Open.
Semana 10: nodos muertos, BlockReport y BlockReceived.
Semana 11: replicacion x3, borrado y re-replicacion por piggyback, ACL.
Semana 12: Raft entre NameNodes, leases que expiran, tokens de bloque.

    python src/namenode/server.py

Un NameNode solo:
    set PORT=50051

Tres NameNodes con Raft (en cada uno, sus propias direcciones):
    set RAFT_SELF=nn1:50052
    set RAFT_PEERS=nn2:50052,nn3:50052
    set RAFT_DIR=data\\nn-1                 journal y dump en disco
    set NAMENODES_CLIENTE=nn1:50052=nn1:50051,nn2:50052=nn2:50051,...
"""

import os
import sys
import time
import uuid
import threading
from concurrent import futures

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import comun, nn, nn_grpc, dn, dn_grpc, ctl, ctl_grpc  # noqa: E402
from common.config import env                            # noqa: E402
from common import seguridad                             # noqa: E402
from common.interfaces import (                          # noqa: E402
    build_auth, build_block_tokens, ConsistentHashPlacer,
    BLOCK_SIZE, REPLICATION_FACTOR)
from namenode.namespace import (                         # noqa: E402
    COMMITTED, UNDER_CONSTRUCTION, R, W, X, MODO_DIR, MODO_ARCHIVO)
from namenode.estado import LocalStore, RaftStore, NoSoyLider  # noqa: E402

NODE_ID = env("NODE_ID", "nn-1")
PORT = env("PORT", "50051")

# Con heartbeats cada 3 s, 30 segundos son diez fallos seguidos: no se
# declara muerto a un nodo por una hipo de la red.
TIMEOUT_DATANODE = int(env("TIMEOUT_DATANODE", "30"))
INTERVALO_VIGILANCIA = 10
INTERVALO_MANTENIMIENTO = int(env("INTERVALO_MANTENIMIENTO", "5"))

# Un lease que nadie renueva en este tiempo se da por muerto, y con el
# su archivo en construccion. El cliente renueva cada LEASE_TTL / 3.
LEASE_TTL = int(env("LEASE_TTL", "300"))

# Cuanto se espera a que un ReplicateCommand se cumpla antes de volver
# a mandarlo (por si el DataNode fuente se cayo a mitad).
ESPERA_REPLICACION = 30


# ---------------------------------------------------------------------
#  Mapeo de errores del namespace a codigos gRPC.
#
#  En un solo sitio a proposito: es lo que el curso evalua como "uso
#  correcto del middleware", y disperso en ifs se vuelve inconsistente.
#
#  | Situacion                                  | StatusCode          |
#  |--------------------------------------------|---------------------|
#  | Token invalido o ausente                   | UNAUTHENTICATED     |
#  | Usuario sin permiso sobre el path          | PERMISSION_DENIED   |
#  | El path (o el padre) no existe             | NOT_FOUND           |
#  | Ya existe                                  | ALREADY_EXISTS      |
#  | Directorio no vacio sin recursivo          | FAILED_PRECONDITION |
#  | Tipo equivocado (rmdir de archivo, etc.)   | INVALID_ARGUMENT    |
#  | No hay DataNodes vivos, o no soy el lider  | UNAVAILABLE         |
#  | Error interno inesperado                   | INTERNAL            |
# ---------------------------------------------------------------------
CODIGOS = {
    "no existe": grpc.StatusCode.NOT_FOUND,
    "el directorio padre no existe": grpc.StatusCode.NOT_FOUND,
    "lease desconocido": grpc.StatusCode.NOT_FOUND,
    "ya existe": grpc.StatusCode.ALREADY_EXISTS,
    "el directorio no esta vacio": grpc.StatusCode.FAILED_PRECONDITION,
    "no se puede borrar la raiz": grpc.StatusCode.FAILED_PRECONDITION,
    "no es un directorio": grpc.StatusCode.INVALID_ARGUMENT,
    "es un directorio, usa rmdir": grpc.StatusCode.INVALID_ARGUMENT,
    "el lease ya no es valido": grpc.StatusCode.FAILED_PRECONDITION,
    "el archivo ya esta completo": grpc.StatusCode.FAILED_PRECONDITION,
    "permiso denegado": grpc.StatusCode.PERMISSION_DENIED,
}


def _mapa_lideres():
    """'raft=cliente,raft=cliente' -> dict. Para que Ping diga a quien ir."""
    salida = {}
    for par in env("NAMENODES_CLIENTE", "").split(","):
        if "=" in par:
            raft, cliente = par.split("=", 1)
            salida[raft.strip()] = cliente.strip()
    return salida


class NameNodeService(nn_grpc.NameNodeServiceServicer):
    def __init__(self, store=None):
        # La costura MetadataStore: LocalStore (uno solo) o RaftStore
        # (replicado). El resto de esta clase no sabe cual tiene.
        self.store = store or LocalStore()
        self.auth = build_auth()
        self.tokens = build_block_tokens()
        self.placer = ConsistentHashPlacer()
        self.lideres = _mapa_lideres()

        # Estado BLANDO: no se replica ni se persiste. Lo reconstruyen los
        # heartbeats y block reports en segundos, en cualquier NameNode.
        self.datanodes = {}       # node_id -> {addr, free_bytes, last_seen}
        self.borrar_en = {}       # node_id -> {block_id} por mandar a borrar
        self.comandos = {}        # node_id -> [Command] por mandar
        self.replicando = {}      # block_id -> hasta cuando esperar
        # Historial de lo agendado para borrar, para diagnostico y pruebas.
        self.pendientes_borrado = []
        self._lock = threading.RLock()

    # El estado durable se lee SIEMPRE a traves del store (ver estado.py).
    @property
    def meta(self):
        return self.store.meta

    @property
    def ns(self):
        return self.store.meta.ns

    @property
    def block_map(self):
        return self.store.meta.block_map

    @property
    def leases(self):
        return self.store.meta.leases

    # ---------------- helpers ----------------

    def _exigir_lider(self, context):
        """Solo el lider atiende. Los demas mandan al cliente a buscarlo.

        Leer en un seguidor podria devolver un namespace atrasado; por
        eso ni siquiera las lecturas se sirven fuera del lider.
        """
        if not self.store.es_lider():
            pista = self.lideres.get(self.store.lider(), "")
            context.abort(grpc.StatusCode.UNAVAILABLE,
                          "no soy el lider" + (": " + pista if pista else ""))

    def _ejecutar(self, context, op, *args):
        try:
            return self.store.ejecutar(op, *args)
        except NoSoyLider as err:
            context.abort(grpc.StatusCode.UNAVAILABLE, str(err))

    def _autorizar(self, token, context):
        """context.abort lanza excepcion: el resto del metodo no corre.

        Devuelve (usuario, grupos). Es mas limpio que set_code + return,
        y evita el error clasico de devolver un mensaje vacio con codigo
        de exito.
        """
        self._exigir_lider(context)
        usuario = self.auth.verify(token)
        if usuario is None:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "token invalido")
        return usuario, self.auth.grupos(usuario)

    @staticmethod
    def _fallar(context, msg):
        context.abort(CODIGOS.get(msg, grpc.StatusCode.INTERNAL), msg)

    def _permiso(self, context, resultado):
        ok, msg = resultado
        if not ok:
            self._fallar(context, msg)

    def _agendar_borrado(self, huerfanos, nodos=None):
        """Los bloques sin dueno se mandan borrar por piggyback.

        Se piden a TODOS los DataNodes conocidos (o a los indicados): el
        NameNode no garantiza saber exactamente quien tiene cada copia, y
        borrar un bloque que no esta es inofensivo. El que se registre
        mas tarde lo reporta en su BlockReport y entra por ahi.
        """
        if not huerfanos:
            return
        with self._lock:
            self.pendientes_borrado.extend(huerfanos)
            for node_id in (nodos or list(self.datanodes)):
                self.borrar_en.setdefault(node_id, set()).update(huerfanos)
                for b in huerfanos:
                    self.replicando.pop(b, None)
        print("[GC] {} bloques huerfanos agendados para borrar".format(
            len(huerfanos)))

    def vivos(self):
        """DataNodes de los que se supo hace menos de TIMEOUT_DATANODE.

        Create usa esto y no datanodes.keys(): es un cambio de una linea
        con consecuencias grandes, porque sin el el NameNode asigna
        bloques a nodos muertos.
        """
        corte = time.time() - TIMEOUT_DATANODE
        return [nid for nid, info in self.datanodes.items()
                if info.get("last_seen", 0) > corte]

    def muertos(self):
        corte = time.time() - TIMEOUT_DATANODE
        return [nid for nid, info in self.datanodes.items()
                if info.get("last_seen", 0) <= corte]

    def _asignar(self, block_id, index, size, vivos):
        destinos = self.placer.place(block_id, vivos, REPLICATION_FACTOR)
        asignacion = nn.BlockAssignment(
            block_id=block_id, index=index, size=size,
            datanodes=[self.datanodes[n]["addr"] for n in destinos],
            access_token=self.tokens.emitir("w", block_id))
        return asignacion, (block_id, index, size, destinos)

    def _vence(self):
        return time.time() + LEASE_TTL

    # ---------------- salud y autenticacion ----------------

    def Ping(self, request, context):
        lider = self.store.es_lider()
        return nn.PingResponse(
            node_id=NODE_ID, is_leader=lider,
            leader_addr="" if lider else self.lideres.get(self.store.lider(), ""))

    def Login(self, request, context):
        # El login no cambia el estado: lo atiende cualquier NameNode.
        token = self.auth.login(request.user, request.password)
        if token is None:
            print("[Login] {} -> rechazado".format(request.user))
            context.abort(grpc.StatusCode.UNAUTHENTICATED,
                          "usuario o contrasena incorrectos")
        print("[Login] {} -> ok".format(request.user))
        vence = getattr(self.auth, "ttl", 0)
        return nn.LoginResponse(
            token=token, expires_at=int(time.time()) + vence)

    # ---------------- RF1: namespace ----------------

    def Mkdir(self, request, context):
        user, grupos = self._autorizar(request.token, context)
        self._permiso(context, self.ns.acceso_padre(request.path, user, grupos))
        ok, msg = self._ejecutar(context, "mkdir", request.path, user,
                                 grupos[0], MODO_DIR, time.time())
        print("[Mkdir] {} ({}) -> {}".format(request.path, user, msg))
        if not ok:
            self._fallar(context, msg)
        return comun.StatusResponse(ok=True, message=msg)

    def Ls(self, request, context):
        user, grupos = self._autorizar(request.token, context)
        self._permiso(context, self.ns.acceso(request.path, user, grupos, R | X))
        with self.meta.lock:
            entries = self.ns.ls(request.path)
            if entries is None:
                context.abort(grpc.StatusCode.NOT_FOUND,
                              "no existe o no es un directorio")
            salida = [comun.Entry(name=e.name, is_dir=e.is_dir, size=e.size,
                                  owner=e.owner, group=e.group, mode=e.mode)
                      for e in entries]
        print("[Ls] {} -> {} entradas".format(request.path, len(salida)))
        return nn.LsResponse(entries=salida)

    def Stat(self, request, context):
        user, grupos = self._autorizar(request.token, context)
        self._permiso(context, self.ns.acceso(request.path, user, grupos, 0))
        with self.meta.lock:
            nodo = self.ns.stat(request.path)
            if nodo is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "no existe")
            return nn.FileInfo(
                path=request.path, is_dir=nodo.is_dir, size=nodo.size,
                num_blocks=len(nodo.blocks), state=nodo.state,
                created_at=int(nodo.created_at), owner=nodo.owner,
                group=nodo.group, mode=nodo.mode)

    def Chmod(self, request, context):
        user, grupos = self._autorizar(request.token, context)
        self._permiso(context, self.ns.acceso(request.path, user, grupos, 0))
        if request.group and request.group not in grupos:
            # Igual que chgrp en Unix: solo a un grupo al que perteneces.
            context.abort(grpc.StatusCode.PERMISSION_DENIED,
                          "no perteneces al grupo " + request.group)
        ok, msg = self._ejecutar(context, "chmod", request.path, user,
                                 request.mode, request.group)
        print("[Chmod] {} {:o} {} -> {}".format(
            request.path, request.mode, request.group, msg))
        if not ok:
            self._fallar(context, msg)
        return comun.StatusResponse(ok=True, message=msg)

    def _borrar(self, op, request, context):
        user, grupos = self._autorizar(request.token, context)
        self._permiso(context, self.ns.acceso_padre(request.path, user, grupos))
        if not self.ns.puede_borrar(request.path, user):
            self._fallar(context, "permiso denegado")
        # PathRequest todavia no lleva el flag recursivo: la v1 de rmdir
        # no es recursiva a proposito (D5).
        ok, msg, huerfanos = self._ejecutar(context, op, request.path)
        print("[{}] {} -> {}".format(op.capitalize(), request.path, msg))
        if not ok:
            self._fallar(context, msg)
        self._agendar_borrado(huerfanos)
        return comun.StatusResponse(ok=True, message=msg)

    def Rmdir(self, request, context):
        return self._borrar("rmdir", request, context)

    def Rm(self, request, context):
        return self._borrar("rm", request, context)

    # ---------------- RF2/RF3: escritura (WORM) ----------------

    def Create(self, request, context):
        """Otorga el lease y dice donde va cada bloque.

        No mueve un solo byte: el NameNode nunca toca el plano de datos.
        Devuelve direcciones (host:puerto) y no node_id, porque es el
        cliente quien va a marcarlas. Cada bloque lleva su token de
        escritura firmado.
        """
        user, grupos = self._autorizar(request.token, context)
        self._permiso(context, self.ns.acceso_padre(request.path, user, grupos))

        if self.ns.stat(request.path) is not None:
            # WORM: no se sobreescribe. Para reemplazar hay que borrar
            # primero, y eso incluye limpiar una subida que quedo a medias.
            self._fallar(context, "ya existe")
        padre, _ = self.ns.parent_of(request.path)
        if padre is None or not padre.is_dir:
            self._fallar(context, "el directorio padre no existe")

        vivos = self.vivos()
        if not vivos:
            context.abort(grpc.StatusCode.UNAVAILABLE,
                          "no hay DataNodes vivos")

        # El ultimo bloque es parcial. Con un contador explicito, y no
        # dividiendo, no hay forma de equivocarse en su tamano.
        asignaciones, bloques = [], []
        restante = request.size
        indice = 0
        while restante > 0:
            tam = min(BLOCK_SIZE, restante)
            a, b = self._asignar(uuid.uuid4().hex, indice, tam, vivos)
            asignaciones.append(a)
            bloques.append(b)
            restante -= tam
            indice += 1

        lease_id = uuid.uuid4().hex
        vence = self._vence()
        ok, msg = self._ejecutar(
            context, "create", request.path, request.size, user, grupos[0],
            MODO_ARCHIVO, lease_id, vence, time.time(), bloques)
        if not ok:
            self._fallar(context, msg)

        print("[Create] {} ({})  {} bytes  {} bloques  lease={}".format(
            request.path, user, request.size, len(asignaciones), lease_id[:8]))
        return nn.CreateResponse(
            lease_id=lease_id, blocks=asignaciones, block_size=BLOCK_SIZE,
            lease_expires_at=int(vence))

    def _lease_propio(self, lease_id, user, context):
        path = self.leases.get(lease_id)
        if path is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "lease desconocido")
        nodo = self.ns.stat(path)
        if nodo is None or nodo.owner != user:
            # El lease es del que hizo el Create, de nadie mas.
            self._fallar(context, "permiso denegado")
        return path, nodo

    def AddBlock(self, request, context):
        user, _ = self._autorizar(request.token, context)
        path, nodo = self._lease_propio(request.lease_id, user, context)
        if request.size == 0 or request.size > BLOCK_SIZE:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "el bloque debe medir entre 1 y {} bytes".format(
                              BLOCK_SIZE))
        vivos = self.vivos()
        if not vivos:
            context.abort(grpc.StatusCode.UNAVAILABLE, "no hay DataNodes vivos")
        a, b = self._asignar(uuid.uuid4().hex, len(nodo.blocks),
                             request.size, vivos)
        ok, msg = self._ejecutar(context, "add_block", request.lease_id, b)
        if not ok:
            self._fallar(context, msg)
        return a

    def Renew(self, request, context):
        user, _ = self._autorizar(request.token, context)
        self._lease_propio(request.lease_id, user, context)
        ok, msg = self._ejecutar(context, "renew", request.lease_id,
                                 self._vence())
        if not ok:
            self._fallar(context, msg)
        return comun.StatusResponse(ok=True, message=msg)

    def Complete(self, request, context):
        """El commit. Aqui el archivo se vuelve visible e inmutable.

        CompleteRequest no lleva token: el lease ES la credencial. Se
        entrego a un usuario ya autenticado en el Create y solo sirve
        para ese path, asi que vale como capability.
        """
        self._exigir_lider(context)
        nodo = self.ns.stat(request.path)
        if nodo is not None and nodo.state == UNDER_CONSTRUCTION \
                and nodo.lease_id == request.lease_id:
            # Verifica que llegue el checksum de cada bloque antes de
            # dar el archivo por bueno.
            llegaron = {c.block_id for c in request.checksums}
            faltan = [b for b in nodo.blocks if b not in llegaron]
            if faltan:
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "faltan los checksums de {} bloques".format(len(faltan)))

        ok, msg = self._ejecutar(
            context, "complete", request.path, request.lease_id,
            {c.block_id: c.sha256 for c in request.checksums})
        if not ok:
            print("[Complete] {} -> {}".format(request.path, msg))
            self._fallar(context, msg)
        print("[Complete] {} -> COMMITTED".format(request.path))
        return comun.StatusResponse(ok=True, message="ok")

    def Abort(self, request, context):
        user, _ = self._autorizar(request.token, context)
        if request.lease_id not in self.leases:
            context.abort(grpc.StatusCode.NOT_FOUND, "lease desconocido")
        self._lease_propio(request.lease_id, user, context)
        ok, msg, huerfanos, path = self._ejecutar(
            context, "abort", request.lease_id)
        if not ok:
            self._fallar(context, msg)
        self._agendar_borrado(huerfanos)
        print("[Abort] {} cancelado".format(path))
        return comun.StatusResponse(ok=True, message="ok")

    # ---------------- RF2/RF3: lectura ----------------

    def Open(self, request, context):
        """Donde esta cada bloque, en orden. El cliente hace el resto."""
        user, grupos = self._autorizar(request.token, context)
        self._permiso(context, self.ns.acceso(request.path, user, grupos, R))
        nodo = self.ns.stat(request.path)
        if nodo is None:
            self._fallar(context, "no existe")
        if nodo.is_dir:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "es un directorio")
        if nodo.state != COMMITTED:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                          "el archivo todavia esta en construccion")

        vivos = set(self.vivos())
        ubicaciones = []
        for block_id in nodo.blocks:
            meta = self.block_map.get(block_id)
            if meta is None:
                context.abort(grpc.StatusCode.INTERNAL,
                              "no hay ubicacion para el bloque " + block_id)
            # Las direcciones se resuelven AL LEER, no al escribir: un
            # DataNode que se reinicio puede anunciarse en otra direccion.
            direcciones = [self.datanodes[n]["addr"]
                           for n in meta["datanodes"] if n in vivos]
            if not direcciones:
                context.abort(
                    grpc.StatusCode.UNAVAILABLE,
                    "ningun DataNode vivo tiene el bloque " + block_id)
            ubicaciones.append(nn.BlockLocation(
                block_id=block_id, index=meta["index"], size=meta["size"],
                datanodes=direcciones, sha256=meta["sha256"],
                access_token=self.tokens.emitir("r", block_id)))

        ubicaciones.sort(key=lambda b: b.index)
        print("[Open] {}  {} bloques".format(request.path, len(ubicaciones)))
        return nn.OpenResponse(size=nodo.size, blocks=ubicaciones)

    # ---------------- mantenimiento (solo el lider) ----------------

    def _bloques_en_construccion(self):
        salida = set()
        for path in list(self.leases.values()):
            nodo = self.ns.stat(path)
            if nodo is not None:
                salida.update(nodo.blocks)
        return salida

    def mantenimiento(self, ahora=None):
        """Una vuelta del recolector y del re-replicador.

        1. Expira los leases que nadie renovo (D8).
        2. Busca bloques con menos replicas vivas que REPLICATION_FACTOR y
           le pide a un DataNode que los tenga que los copie a otros. El
           pedido viaja piggyback en su proximo heartbeat.
        """
        if not self.store.es_lider():
            return
        ahora = ahora or time.time()

        try:
            vencidos = self.store.ejecutar("expirar", ahora)
        except NoSoyLider:
            return
        for path, huerfanos in vencidos:
            print("[lease] vencio la subida de {}".format(path))
            self._agendar_borrado(huerfanos)

        vivos = self.vivos()
        meta_vivos = set(vivos)
        objetivo = min(REPLICATION_FACTOR, len(vivos))
        en_construccion = self._bloques_en_construccion()
        with self.meta.lock:
            bloques = list(self.block_map.items())
        for block_id, meta in bloques:
            if block_id in en_construccion:
                continue
            con_copia = [n for n in meta["datanodes"] if n in meta_vivos]
            faltan = objetivo - len(con_copia)
            if faltan <= 0:
                self.replicando.pop(block_id, None)
                continue
            if not con_copia:
                print("[replicacion] el bloque {} no tiene copias vivas".format(
                    block_id[:12]))
                continue
            if self.replicando.get(block_id, 0) > ahora:
                continue          # ya se pidio; se espera a que llegue
            # Ni los que ya lo tienen, ni los que tienen pendiente borrarlo
            # (una copia descartada): el borrado llegaria despues y se
            # llevaria la copia buena.
            candidatos = [n for n in self.placer.place(block_id, vivos, len(vivos))
                          if n not in con_copia
                          and block_id not in self.borrar_en.get(n, ())][:faltan]
            if not candidatos:
                continue
            fuente = con_copia[0]
            cmd = ctl.Command(replicate=ctl.ReplicateCommand(
                block_id=block_id,
                targets=[self.datanodes[n]["addr"] for n in candidatos],
                access_token=self.tokens.emitir("w", block_id)))
            with self._lock:
                self.comandos.setdefault(fuente, []).append(cmd)
                self.replicando[block_id] = ahora + ESPERA_REPLICACION
            print("[replicacion] {} tiene {} copias: {} lo copia a {}".format(
                block_id[:12], len(con_copia), fuente, candidatos))

    def comandos_para(self, node_id):
        """Lo que viaja piggyback en la respuesta al heartbeat de node_id."""
        with self._lock:
            salida = self.comandos.pop(node_id, [])
            borrar = self.borrar_en.pop(node_id, set())
        if borrar:
            salida.append(ctl.Command(remove=ctl.DeleteCommand(
                block_ids=sorted(borrar))))
        return salida


class ControlService(ctl_grpc.ControlServiceServicer):
    """El DataNode siempre inicia la conexion. El NameNode nunca al reves.

    Lo atienden TODOS los NameNodes, no solo el lider: asi el que gane
    una eleccion ya sabe que DataNodes estan vivos y que bloques tienen.
    Pero solo el lider manda comandos y agenda borrados: un seguidor
    atrasado podria creer huerfano un bloque recien creado.
    """

    def __init__(self, nn):
        self.nn = nn

    def Heartbeat(self, request, context):
        anterior = self.nn.datanodes.get(request.node_id)
        self.nn.datanodes[request.node_id] = {
            "addr": request.addr,
            "free_bytes": request.free_bytes,
            "num_blocks": request.num_blocks,
            "last_seen": time.time(),
        }
        if anterior is None:
            print("[Heartbeat] nuevo DataNode: {} en {}".format(
                request.node_id, request.addr))
        elif anterior.get("last_seen", 0) <= time.time() - TIMEOUT_DATANODE:
            print("[Heartbeat] {} revivio".format(request.node_id))
        lider = self.nn.store.es_lider()
        comandos = self.nn.comandos_para(request.node_id) if lider else []
        return ctl.HeartbeatResponse(commands=comandos, is_leader=lider)

    def BlockReceived(self, request, context):
        """El DataNode avisa que ya tiene un bloque.

        Sirve para confirmar que quedo donde el NameNode esperaba: si un
        nodo reporta un bloque que no se le asigno, se anota igual, porque
        el que manda es el disco.
        """
        with self.nn.meta.lock:
            meta = self.nn.block_map.get(request.block_id)
            if meta is None:
                print("[BlockReceived] {} reporto un bloque desconocido: {}".format(
                    request.node_id, request.block_id))
                return comun.StatusResponse(ok=True, message="bloque sin dueno")
            corrupta = bool(meta["sha256"] and request.sha256
                            and request.sha256 != meta["sha256"])
            if corrupta:
                # Una copia distinta de la confirmada en el Complete: no
                # cuenta como replica y se manda borrar. La re-replicacion
                # pone una buena en su lugar.
                if request.node_id in meta["datanodes"]:
                    meta["datanodes"].remove(request.node_id)
            elif request.node_id not in meta["datanodes"]:
                meta["datanodes"].append(request.node_id)
            if request.sha256 and not meta["sha256"]:
                meta["sha256"] = request.sha256
        if corrupta:
            print("[BlockReceived] {} tiene una copia distinta de {}: se borra".format(
                request.node_id, request.block_id[:12]))
            if self.nn.store.es_lider():
                self.nn._agendar_borrado([request.block_id], [request.node_id])
            return comun.StatusResponse(ok=True, message="copia descartada")
        return comun.StatusResponse(ok=True, message="ok")

    def BlockReport(self, request, context):
        """La lista completa de bloques de un DataNode.

        Esto es lo que permite NO persistir las ubicaciones: lo durable es
        el namespace (Raft), pero el mapa blockID -> DataNodes se
        reconstruye solo con los reports al arrancar. Es lo que hace HDFS.
        """
        reportados = set(request.block_ids)
        conocidos = 0
        with self.nn.meta.lock:
            for block_id, meta in self.nn.block_map.items():
                if block_id in reportados:
                    conocidos += 1
                    if request.node_id not in meta["datanodes"]:
                        meta["datanodes"].append(request.node_id)
                elif request.node_id in meta["datanodes"]:
                    # El nodo ya no lo tiene: dejo de contar como replica.
                    meta["datanodes"].remove(request.node_id)
            huerfanos = [b for b in reportados if b not in self.nn.block_map]

        print("[BlockReport] {}: {} bloques ({} conocidos, {} huerfanos)".format(
            request.node_id, len(reportados), conocidos, len(huerfanos)))
        if self.nn.store.es_lider():
            # Los huerfanos son de archivos ya borrados: a la cola de
            # borrado, solo de ESE nodo, que es el que los tiene.
            self.nn._agendar_borrado(huerfanos, [request.node_id])
        return comun.StatusResponse(ok=True, message="ok")


def vigilar(servicio):
    """Anuncia por consola cuando un DataNode pasa a muerto o revive.

    No decide nada: vivos() ya filtra por last_seen cada vez que se
    consulta. Esto es para verlo en la demo.
    """
    caidos = set()
    while True:
        time.sleep(INTERVALO_VIGILANCIA)
        ahora = set(servicio.muertos())
        for nid in ahora - caidos:
            print("[vigilancia] {} no responde hace mas de {} s".format(
                nid, TIMEOUT_DATANODE))
        for nid in caidos - ahora:
            print("[vigilancia] {} volvio".format(nid))
        caidos = ahora


def mantener(servicio):
    era_lider = None
    while True:
        time.sleep(INTERVALO_MANTENIMIENTO)
        lider = servicio.store.es_lider()
        if lider != era_lider:
            print("[raft] {} {}".format(
                NODE_ID, "es el LIDER" if lider else "es seguidor"))
            era_lider = lider
        try:
            servicio.mantenimiento()
        except Exception as err:          # que un error no mate el hilo
            print("[mantenimiento] error: {}".format(err))


def construir_store():
    yo = env("RAFT_SELF", "")
    if not yo:
        print("[raft] sin RAFT_SELF: NameNode unico, sin replicacion")
        return LocalStore()
    otros = [p.strip() for p in env("RAFT_PEERS", "").split(",") if p.strip()]
    carpeta = env("RAFT_DIR", os.path.join("data", NODE_ID))
    print("[raft] {} con {} ; journal en {}".format(yo, otros, carpeta))
    return RaftStore(yo, otros, carpeta, env("RAFT_PASSWORD", "") or None)


def serve():
    server = seguridad.servidor(futures.ThreadPoolExecutor(max_workers=16))
    servicio = NameNodeService(construir_store())
    threading.Thread(target=vigilar, args=(servicio,), daemon=True).start()
    threading.Thread(target=mantener, args=(servicio,), daemon=True).start()
    nn_grpc.add_NameNodeServiceServicer_to_server(servicio, server)
    ctl_grpc.add_ControlServiceServicer_to_server(
        ControlService(servicio), server)
    seguridad.abrir_puerto(server, "[::]:" + PORT)
    server.start()
    print("NameNode {} escuchando en el puerto {}{}".format(
        NODE_ID, PORT, " (mTLS)" if seguridad.tls_activo() else ""))
    print("Ctrl+C para detener.")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nDeteniendo NameNode.")


if __name__ == "__main__":
    serve()
