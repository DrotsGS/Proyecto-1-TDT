"""
DataNode de DFSha.

Semana 6:  se registra ante el NameNode por heartbeat cada 3 segundos.
Semana 8:  PutBlock y GetBlock con streaming.
Semana 10: BlockReport y BlockReceived.
Semana 11: pipeline de replicacion, DeleteBlock y ReplicateTo, y los
           comandos piggyback del heartbeat.
Semana 12: tokens de bloque, mTLS, y latido hacia TODOS los NameNodes.

    python src/datanode/server.py

    set NAMENODE_ADDR=nn1:50051,nn2:50051,nn3:50051
"""

import os
import sys
import time
import shutil
import hashlib
import threading
from concurrent import futures

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import comun, nn, nn_grpc, dn, dn_grpc, ctl, ctl_grpc  # noqa: E402
from common.config import env, data_dir           # noqa: E402
from common import seguridad                      # noqa: E402
from common.interfaces import (                   # noqa: E402
    CHUNK_SIZE, build_block_tokens, PipelineReplicator)

NODE_ID = env("NODE_ID", "dn-1")
PORT = env("PORT", "50060")
ADDR = env("ADVERTISE_ADDR", "localhost:" + PORT)
NAMENODES = [a.strip() for a in env("NAMENODE_ADDR", "localhost:50051").split(",")
             if a.strip()]
DATA_DIR = data_dir(NODE_ID)


def _leer_bloque(ruta):
    with open(ruta, "rb") as f:
        while True:
            datos = f.read(CHUNK_SIZE)
            if not datos:
                return
            yield datos


def enviar_bloque(direccion, block_id, ruta, token, pipeline):
    """Manda un bloque que ya esta en disco a otro DataNode.

    Lo usan el pipeline (reenviar lo que acaba de llegar) y la
    re-replicacion (copiar lo que pide el NameNode). Devuelve las
    replicas que confirmo la cadena.
    """
    canal = seguridad.canal(direccion)
    try:
        stub = dn_grpc.DataNodeServiceStub(canal)

        def stream():
            yield dn.BlockChunk(header=dn.BlockHeader(
                block_id=block_id, size=os.path.getsize(ruta),
                access_token=token, pipeline=pipeline))
            for datos in _leer_bloque(ruta):
                yield dn.BlockChunk(data=datos)

        return list(stub.PutBlock(stream(), timeout=600).replicas)
    finally:
        canal.close()


class DataNodeService(dn_grpc.DataNodeServiceServicer):
    """El almacen de bloques.

    La carpeta se recibe por parametro y no se lee del entorno aqui
    dentro: asi las pruebas pueden levantar varios DataNodes en un mismo
    proceso, cada uno con su disco.
    """

    def __init__(self, carpeta=None, avisar=None, tokens=None, direccion=None):
        self.carpeta = carpeta or DATA_DIR
        # Callback para avisarle al NameNode que llego un bloque. Se
        # inyecta para que las pruebas puedan levantar un DataNode sin
        # NameNode detras.
        self.avisar = avisar
        # El DataNode valida los tokens SOLO, con el secreto compartido.
        # Nunca le pregunta al NameNode: no lo vuelve cuello de botella.
        self.tokens = tokens or build_block_tokens()
        self.direccion = direccion or ADDR
        self.replicador = PipelineReplicator()

    def bloques(self):
        return [f[4:] for f in os.listdir(self.carpeta)
                if f.startswith("blk_")]

    def ruta_bloque(self, block_id):
        return os.path.join(self.carpeta, "blk_" + block_id)

    def ruta_temporal(self, block_id):
        # Los tmp_ no empiezan por blk_, asi que el heartbeat no los
        # cuenta como bloques hasta que esten completos.
        return os.path.join(self.carpeta, "tmp_{}_{}".format(
            block_id, threading.get_ident()))

    def _validar(self, context, token, op, block_id):
        if not self.tokens.verificar(token, op, block_id):
            context.abort(grpc.StatusCode.PERMISSION_DENIED,
                          "token de bloque invalido o vencido")

    def PutBlock(self, request_iterator, context):
        """Recibe un bloque por streaming y lo deja en disco.

        El primer mensaje del stream trae el BlockHeader; los siguientes,
        bytes. Nada se acumula en memoria: cada chunk se escribe apenas
        llega y el sha256 se calcula de forma incremental.

        Se escribe a tmp_<id> y se renombra con os.replace al terminar.
        El renombrado es atomico en Windows y en Linux, asi que en disco
        nunca hay un blk_<id> a medias: o esta completo, o no esta.

        Si el header trae pipeline, despues de guardar el bloque se lo
        pasa al siguiente DataNode (semana 11). El cliente lo sube una
        sola vez y aun asi quedan tres copias.
        """
        header = None
        digest = hashlib.sha256()
        recibidos = 0
        f = None
        temporal = None

        try:
            for chunk in request_iterator:
                if chunk.HasField("header"):
                    if header is not None:
                        context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                                      "el header llego dos veces")
                    header = chunk.header
                    self._validar(context, header.access_token, "w",
                                  header.block_id)
                    temporal = self.ruta_temporal(header.block_id)
                    f = open(temporal, "wb")
                    continue

                if header is None:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                                  "el primer mensaje debe ser el header")

                datos = chunk.data
                recibidos += len(datos)
                if recibidos > header.size:
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "llegaron mas bytes de los anunciados ({} > {})".format(
                            recibidos, header.size))
                digest.update(datos)
                f.write(datos)

            if header is None:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                              "stream vacio, sin header")
            if recibidos != header.size:
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "se anunciaron {} bytes y llegaron {}".format(
                        header.size, recibidos))

            f.close()
            f = None
            final = self.ruta_bloque(header.block_id)
            os.replace(temporal, final)
            sha = digest.hexdigest()
            print("[PutBlock] {}  {} bytes  sha256={}".format(
                header.block_id, recibidos, sha[:12]))
            if self.avisar is not None:
                # BlockReceived: el NameNode se entera por el DataNode, no
                # por el cliente. El que sabe que el bloque esta en disco
                # es quien lo escribio.
                self.avisar(header.block_id, sha)

            replicas = [self.direccion]
            if header.pipeline:
                replicas += self._reenviar(header, final)
            return dn.PutBlockResponse(ok=True, sha256=sha, message="ok",
                                       replicas=replicas)

        finally:
            if f is not None:
                f.close()
            # Si la subida se corto, no dejes basura ocupando disco.
            if temporal and os.path.exists(temporal):
                try:
                    os.remove(temporal)
                except OSError:
                    pass

    def _reenviar(self, header, ruta):
        """Pasa el bloque por el pipeline.

        Si el siguiente no responde, se salta y se intenta con el que
        sigue: se pierde una copia, no la escritura. La que falte la
        repone la re-replicacion del NameNode.
        """
        for siguiente, resto in self.replicador.intentos(list(header.pipeline)):
            try:
                return enviar_bloque(siguiente, header.block_id, ruta,
                                     header.access_token, resto)
            except grpc.RpcError as err:
                print("[pipeline] {} no recibio {}: {}".format(
                    siguiente, header.block_id[:12], err.code().name))
        return []

    def GetBlock(self, request, context):
        """Devuelve el bloque en trozos de CHUNK_SIZE. Es un generador."""
        self._validar(context, request.access_token, "r", request.block_id)
        ruta = self.ruta_bloque(request.block_id)
        if not os.path.exists(ruta):
            context.abort(grpc.StatusCode.NOT_FOUND,
                          "este DataNode no tiene el bloque " + request.block_id)
        print("[GetBlock] {}  {} bytes".format(
            request.block_id, os.path.getsize(ruta)))
        for datos in _leer_bloque(ruta):
            yield dn.BlockChunk(data=datos)

    def borrar(self, block_ids):
        borrados = 0
        for block_id in block_ids:
            try:
                os.remove(self.ruta_bloque(block_id))
                borrados += 1
            except FileNotFoundError:
                pass          # borrar lo que no esta es inofensivo
        if borrados:
            print("[GC] {} bloques borrados".format(borrados))
        return borrados

    def DeleteBlock(self, request, context):
        self._validar(context, request.access_token, "w", request.block_id)
        self.borrar([request.block_id])
        return comun.StatusResponse(ok=True, message="ok")

    def replicar(self, block_id, targets, token):
        """Copia un bloque local a otros DataNodes (re-replicacion)."""
        ruta = self.ruta_bloque(block_id)
        if not os.path.exists(ruta):
            return []
        for head, resto in self.replicador.intentos(list(targets)):
            try:
                hechas = enviar_bloque(head, block_id, ruta, token, resto)
                print("[replicacion] {} copiado a {}".format(
                    block_id[:12], hechas))
                return hechas
            except grpc.RpcError as err:
                print("[replicacion] {} fallo: {}".format(head, err.code().name))
        return []

    def ReplicateTo(self, request, context):
        self._validar(context, request.access_token, "w", request.block_id)
        if not os.path.exists(self.ruta_bloque(request.block_id)):
            context.abort(grpc.StatusCode.NOT_FOUND,
                          "este DataNode no tiene el bloque " + request.block_id)
        hechas = self.replicar(request.block_id, request.targets,
                               request.access_token)
        return comun.StatusResponse(
            ok=bool(hechas), message="copiado a {}".format(", ".join(hechas)))

    def ejecutar(self, comando):
        """Un comando que llego piggyback en la respuesta al heartbeat."""
        if comando.HasField("remove"):
            self.borrar(comando.remove.block_ids)
        elif comando.HasField("replicate"):
            r = comando.replicate
            # En otro hilo: copiar 128 MB no puede frenar los latidos.
            threading.Thread(
                target=self.replicar,
                args=(r.block_id, list(r.targets), r.access_token),
                daemon=True).start()


INTERVALO_HEARTBEAT = 3
INTERVALO_BLOCK_REPORT = 60


_stubs = {}


def control_stub(direccion):
    if direccion not in _stubs:
        _stubs[direccion] = ctl_grpc.ControlServiceStub(seguridad.canal(direccion))
    return _stubs[direccion]


def heartbeat_loop(servicio, direccion):
    """El DataNode reporta su propia direccion, a UN NameNode.

    Solo el es capaz de saber por donde lo alcanzan los clientes: en
    Windows es localhost, en Docker el nombre del servicio, en EC2 una
    IP privada. Por eso el NameNode no la adivina, se la preguntan.

    Hay un hilo de estos por cada NameNode, para que cualquiera que
    quede de lider ya sepa que el DataNode esta vivo. Un hilo por
    NameNode, y no un bucle que los recorra: si uno esta caido, esperar
    su timeout atrasaria el latido hacia el lider, y el lider declararia
    muerto a un DataNode sano. Solo se obedecen comandos del lider.
    """
    stub = control_stub(direccion)
    while True:
        try:
            pedido = ctl.HeartbeatRequest(
                node_id=NODE_ID, addr=ADDR,
                free_bytes=shutil.disk_usage(servicio.carpeta).free,
                num_blocks=len(servicio.bloques()))
            resp = stub.Heartbeat(pedido, timeout=INTERVALO_HEARTBEAT)
            if resp.is_leader:
                for cmd in resp.commands:
                    servicio.ejecutar(cmd)
        except grpc.RpcError as e:
            print("[heartbeat] {} no responde: {}".format(
                direccion, e.code().name))
        except OSError as e:
            print("[heartbeat] no se pudo leer el disco: {}".format(e))
        time.sleep(INTERVALO_HEARTBEAT)


def block_report_loop(servicio, direccion):
    """La lista completa de bloques, al arrancar y cada minuto.

    Con esto el NameNode reconstruye el mapa de ubicaciones sin tener que
    persistirlo: si se reinicia, en un minuto sabe donde esta todo.
    """
    stub = control_stub(direccion)
    while True:
        ids = servicio.bloques()
        try:
            stub.BlockReport(ctl.BlockReportRequest(
                node_id=NODE_ID, block_ids=ids), timeout=10)
            print("[BlockReport] {} bloques reportados a {}".format(
                len(ids), direccion))
        except grpc.RpcError as e:
            print("[BlockReport] {} no recibio: {}".format(
                direccion, e.code().name))
        time.sleep(INTERVALO_BLOCK_REPORT)


def _avisar(direccion, pedido):
    try:
        control_stub(direccion).BlockReceived(pedido, timeout=5)
    except grpc.RpcError as e:
        # Que falle el aviso no invalida el bloque: el proximo
        # BlockReport lo vuelve a contar.
        print("[BlockReceived] {} no recibio: {}".format(
            direccion, e.code().name))


def avisar_bloque_recibido(block_id, sha):
    """En segundo plano: un NameNode caido no puede frenar un PutBlock."""
    pedido = ctl.BlockReceivedRequest(
        node_id=NODE_ID, block_id=block_id, sha256=sha)
    for direccion in NAMENODES:
        threading.Thread(target=_avisar, args=(direccion, pedido),
                         daemon=True).start()


def serve():
    os.makedirs(DATA_DIR, exist_ok=True)
    servicio = DataNodeService(DATA_DIR, avisar=avisar_bloque_recibido)
    for direccion in NAMENODES:
        threading.Thread(target=heartbeat_loop, args=(servicio, direccion),
                         daemon=True).start()
        threading.Thread(target=block_report_loop, args=(servicio, direccion),
                         daemon=True).start()

    server = seguridad.servidor(futures.ThreadPoolExecutor(max_workers=16))
    dn_grpc.add_DataNodeServiceServicer_to_server(servicio, server)
    seguridad.abrir_puerto(server, "[::]:" + PORT)
    server.start()
    print("DataNode {} escuchando en el puerto {}{}".format(
        NODE_ID, PORT, " (mTLS)" if seguridad.tls_activo() else ""))
    print("  se anuncia como : {}".format(ADDR))
    print("  NameNodes       : {}".format(", ".join(NAMENODES)))
    print("  bloques en      : {}".format(DATA_DIR))
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nDeteniendo DataNode.")


if __name__ == "__main__":
    serve()
