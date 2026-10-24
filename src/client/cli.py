"""
Cliente DFSha.

Semana 6:  ping, mkdir, ls.
Semana 7:  login, rmdir, rm, stat.
Semana 8:  put y get.
Semana 10: put y get en paralelo.
Semana 11: cd, pwd, chmod; escritura por pipeline a tres replicas.
Semana 12: varios NameNodes (busca al lider), cifrado AES-GCM en el
           cliente, tokens de bloque y mTLS.

    python src/client/cli.py login drots
    python src/client/cli.py mkdir /docs
    python src/client/cli.py cd /docs
    python src/client/cli.py put informe.pdf informe.pdf
    python src/client/cli.py ls

    set NAMENODE_ADDR=nn1:50051,nn2:50051,nn3:50051
"""

import os
import sys
import time
import getpass
import hashlib
import argparse
import posixpath
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import comun, nn, nn_grpc, dn, dn_grpc, ctl, ctl_grpc  # noqa: E402
from common.config import (                                  # noqa: E402
    env, read_token, token_file, read_cwd, write_cwd)
from common import seguridad                                 # noqa: E402
from common.interfaces import (                              # noqa: E402
    CHUNK_SIZE, build_cipher, PipelineReplicator)

NAMENODES = [a.strip() for a in env("NAMENODE_ADDR", "localhost:50051").split(",")
             if a.strip()]
ROL_TLS = env("DFSHA_CERT", "cliente")

# Cuantos bloques viajan a la vez. Con un DataNode da igual; con cuatro
# es lo que multiplica el throughput.
MAX_PARALELO = int(env("DFSHA_MAX_PARALELO", "4"))

REPLICADOR = PipelineReplicator()


def token():
    """Se lee en cada llamada, no al importar: el login lo cambia."""
    return read_token()


_cifrador = None
_lock_cifrador = threading.Lock()


def cipher():
    """AES-GCM con la clave del usuario (ver interfaces.build_cipher).

    Con lock: los bloques suben en paralelo y la primera vez todos los
    hilos llegan aqui juntos; solo uno debe crear la clave.
    """
    global _cifrador
    with _lock_cifrador:
        if _cifrador is None:
            _cifrador = build_cipher()
        return _cifrador


def ruta(path):
    """Resuelve un path relativo contra el directorio actual (cd)."""
    if not path:
        return read_cwd()
    if not path.startswith("/"):
        path = posixpath.join(read_cwd(), path)
    normal = posixpath.normpath(path)
    return "/" if normal in ("", ".") else "/" + normal.lstrip("/")


# ---------------------------------------------------------------------
#  Varios NameNodes: siempre se habla con el lider
# ---------------------------------------------------------------------

class Lider:
    """Un stub que siempre apunta al NameNode lider.

    Pregunta con Ping quien es el lider y le habla a ese. Si la llamada
    falla con UNAVAILABLE (el lider se cayo, o el que creiamos lider ya
    no lo es), vuelve a buscar y reintenta. Desde afuera se usa igual
    que un NameNodeServiceStub: lider.Mkdir(...).

    Es la transparencia de localizacion del lado del control (RNF7): el
    usuario no sabe ni le importa cual de los tres NameNodes contesto.
    """

    INTENTOS = 8
    ESPERA = 1.0

    def __init__(self, direcciones):
        self.direcciones = list(direcciones)
        self._stub = None
        self._canales = []

    def _buscar(self):
        for c in self._canales:
            c.close()
        self._canales = []
        for direccion in self.direcciones:
            canal = seguridad.canal(direccion, ROL_TLS)
            self._canales.append(canal)
            stub = nn_grpc.NameNodeServiceStub(canal)
            if len(self.direcciones) == 1:
                return stub
            try:
                r = stub.Ping(comun.Empty(), timeout=2)
            except grpc.RpcError:
                continue
            if r.is_leader:
                return stub
        return None

    def __getattr__(self, nombre):
        def llamada(pedido, **kw):
            ultimo = None
            for _ in range(self.INTENTOS):
                if self._stub is None:
                    self._stub = self._buscar()
                if self._stub is None:
                    time.sleep(self.ESPERA)    # eleccion en curso
                    continue
                try:
                    return getattr(self._stub, nombre)(pedido, **kw)
                except grpc.RpcError as err:
                    if err.code() != grpc.StatusCode.UNAVAILABLE \
                            or "DataNode" in (err.details() or ""):
                        raise
                    ultimo = err
                    self._stub = None
                    time.sleep(self.ESPERA)
            if ultimo is not None:
                raise ultimo
            raise RuntimeError("ningun NameNode es lider: {}".format(
                ", ".join(self.direcciones)))
        return llamada

    def cerrar(self):
        for c in self._canales:
            c.close()


def build_parser():
    ap = argparse.ArgumentParser(
        prog="dfsha", description="Cliente del sistema de archivos DFSha")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("ping", help="verifica que el NameNode responde")

    p = sub.add_parser("login", help="pide un token y lo guarda")
    p.add_argument("usuario")
    p.add_argument("--password", help="si no lo pasas, se pide sin eco")

    p = sub.add_parser("mkdir", help="crea un directorio")
    p.add_argument("path")

    p = sub.add_parser("ls", help="lista un directorio")
    p.add_argument("path", nargs="?", default="")

    p = sub.add_parser("cd", help="cambia el directorio actual")
    p.add_argument("path", nargs="?", default="/")

    sub.add_parser("pwd", help="muestra el directorio actual")

    p = sub.add_parser("rmdir", help="borra un directorio vacio")
    p.add_argument("path")

    p = sub.add_parser("rm", help="borra un archivo")
    p.add_argument("path")

    p = sub.add_parser("stat", help="muestra los metadatos de una ruta")
    p.add_argument("path")

    p = sub.add_parser("chmod", help="cambia permisos (y grupo) de una ruta")
    p.add_argument("modo", help="octal, por ejemplo 750")
    p.add_argument("path")
    p.add_argument("--grupo", default="", help="nuevo grupo")

    p = sub.add_parser("put", help="sube un archivo")
    p.add_argument("local")
    p.add_argument("remoto")

    p = sub.add_parser("get", help="baja un archivo")
    p.add_argument("remoto")
    p.add_argument("local")
    return ap


def _modo(m):
    """0o750 -> 'rwxr-x---', con la t del sticky como en ls -l."""
    letras = ""
    for corrimiento in (6, 3, 0):
        bits = (m >> corrimiento) & 7
        letras += ("r" if bits & 4 else "-") + ("w" if bits & 2 else "-") \
            + ("x" if bits & 1 else "-")
    if m & 0o1000:
        letras = letras[:-1] + ("t" if letras[-1] == "x" else "T")
    return letras


def cmd_ping(stub, args):
    r = stub.Ping(comun.Empty())
    print("{}   lider={}".format(r.node_id, r.is_leader))


def cmd_login(stub, args):
    password = args.password or getpass.getpass("contrasena: ")
    r = stub.Login(nn.LoginRequest(
        user=args.usuario, password=password))
    destino = token_file()
    with open(destino, "w", encoding="utf-8") as f:
        f.write(r.token)
    try:
        os.chmod(destino, 0o600)     # en Windows es casi simbolico
    except OSError:
        pass
    print("token guardado en {}".format(destino))


def cmd_mkdir(stub, args):
    r = stub.Mkdir(nn.PathRequest(path=ruta(args.path), token=token()))
    print(r.message)


def cmd_ls(stub, args):
    r = stub.Ls(nn.PathRequest(path=ruta(args.path), token=token()))
    if not r.entries:
        print("(vacio)")
        return
    for e in r.entries:
        tipo = "d" if e.is_dir else "-"
        print("{}{} {:<10} {:<10} {:>12}  {}".format(
            tipo, _modo(e.mode), e.owner, e.group, e.size, e.name))


def cmd_cd(stub, args):
    destino = ruta(args.path)
    r = stub.Stat(nn.PathRequest(path=destino, token=token()))
    if not r.is_dir:
        print("{} no es un directorio".format(destino))
        sys.exit(1)
    # Que se pueda listar: si no, el cd te deja en un sitio inutil.
    stub.Ls(nn.PathRequest(path=destino, token=token()))
    write_cwd(destino)
    print(destino)


def cmd_pwd(stub, args):
    print(read_cwd())


def cmd_rmdir(stub, args):
    r = stub.Rmdir(nn.PathRequest(path=ruta(args.path), token=token()))
    print(r.message)


def cmd_rm(stub, args):
    r = stub.Rm(nn.PathRequest(path=ruta(args.path), token=token()))
    print(r.message)


def cmd_stat(stub, args):
    r = stub.Stat(nn.PathRequest(path=ruta(args.path), token=token()))
    tipo = "directorio" if r.is_dir else "archivo"
    estado = "COMMITTED" if r.state == comun.COMMITTED \
        else "UNDER_CONSTRUCTION"
    print("ruta    : {}".format(r.path))
    print("tipo    : {}".format(tipo))
    print("tamano  : {} bytes".format(r.size))
    print("bloques : {}".format(r.num_blocks))
    print("estado  : {}".format(estado))
    print("dueno   : {}  grupo: {}".format(r.owner, r.group))
    print("permisos: {} ({:o})".format(_modo(r.mode), r.mode))


def cmd_chmod(stub, args):
    r = stub.Chmod(nn.ChmodRequest(
        path=ruta(args.path), mode=int(args.modo, 8), group=args.grupo,
        token=token()))
    print(r.message)


# ---------------------------------------------------------------------
#  RF2: transferencia de archivos (plano de datos)
#
#  El NameNode no aparece en ninguna de estas funciones mas que para
#  preguntarle donde poner o donde buscar. Los bytes van del cliente al
#  DataNode y de vuelta, directo.
#
#  Cifrado (semana 12): cada chunk de CHUNK_SIZE bytes claros se cifra
#  por separado con AES-GCM y viaja como nonce + cifrado + tag. El
#  DataNode guarda eso tal cual: nunca ve texto claro. El sha256 que
#  verifica el DataNode, y que guarda el NameNode, es el del CIFRADO; la
#  integridad del texto claro la da el tag de GCM.
# ---------------------------------------------------------------------

def _canal_datanode(direccion):
    return seguridad.canal(direccion, ROL_TLS)


def _aad(block_id, i):
    return "{}:{}".format(block_id, i).encode()


def _leer_tramo(ruta_local, offset, cuantos):
    """Genera el tramo [offset, offset+cuantos) en trozos de CHUNK_SIZE.

    Con seek, no leyendo el archivo entero: un archivo de 500 MB no cabe
    comodo en memoria y no tiene por que caber.
    """
    with open(ruta_local, "rb") as f:
        f.seek(offset)
        restante = cuantos
        while restante > 0:
            datos = f.read(min(CHUNK_SIZE, restante))
            if not datos:
                return
            restante -= len(datos)
            yield datos


def _put_a_un_datanode(direccion, pipeline, asignacion, ruta_local, offset):
    """Sube un bloque al head del pipeline y verifica el sha256."""
    canal = _canal_datanode(direccion)
    c = cipher()
    try:
        stub = dn_grpc.DataNodeServiceStub(canal)
        digest = hashlib.sha256()

        def stream():
            yield dn.BlockChunk(header=dn.BlockHeader(
                block_id=asignacion.block_id,
                size=c.tam_cifrado(asignacion.size, CHUNK_SIZE),
                access_token=asignacion.access_token,
                pipeline=pipeline))
            for i, claro in enumerate(
                    _leer_tramo(ruta_local, offset, asignacion.size)):
                datos = c.encrypt(claro, _aad(asignacion.block_id, i))
                digest.update(datos)
                yield dn.BlockChunk(data=datos)

        respuesta = stub.PutBlock(stream())
        mio = digest.hexdigest()
        if respuesta.sha256 != mio:
            raise RuntimeError(
                "el bloque {} llego corrupto a {} ({} != {})".format(
                    asignacion.block_id, direccion,
                    respuesta.sha256[:12], mio[:12]))
        return respuesta
    finally:
        canal.close()


def subir_bloque(asignacion, ruta_local, offset):
    """Sube el bloque una vez; el pipeline de DataNodes hace las copias.

    Si el head no responde, el siguiente pasa a ser head con el resto
    del pipeline (PipelineReplicator.intentos). Basta con que UNA copia
    quede: la re-replicacion del NameNode repone las que falten.
    """
    ultimo_error = None
    for head, pipeline in REPLICADOR.intentos(list(asignacion.datanodes)):
        try:
            return _put_a_un_datanode(head, pipeline, asignacion,
                                      ruta_local, offset)
        except (grpc.RpcError, RuntimeError) as err:
            print("  aviso: {} fallo ({})".format(head, err))
            ultimo_error = err
    raise RuntimeError(
        "ningun DataNode acepto el bloque {}: {}".format(
            asignacion.block_id, ultimo_error))


def _marcos(stream, block_id, size, c):
    """Rearma los chunks cifrados y los descifra en orden.

    El DataNode manda trozos de CHUNK_SIZE bytes de lo que tiene en
    disco, que no coinciden con los marcos cifrados (CHUNK_SIZE + 28).
    Como el tamano claro del bloque se conoce, el largo de cada marco
    tambien: se acumula hasta completarlo y se descifra.
    """
    buffer = bytearray()
    restante = size
    i = 0
    for chunk in stream:
        buffer.extend(chunk.data)
        while restante > 0:
            largo = min(CHUNK_SIZE, restante) + c.OVERHEAD
            if len(buffer) < largo:
                break
            marco = bytes(buffer[:largo])
            del buffer[:largo]
            yield marco, c.decrypt(marco, _aad(block_id, i))
            restante -= largo - c.OVERHEAD
            i += 1
    if restante > 0 or buffer:
        raise RuntimeError("el bloque {} llego incompleto".format(block_id))


def bajar_bloque(ubicacion, ruta_local, offset):
    """Baja un bloque y lo escribe EN SU SITIO dentro del archivo.

    Nada de acumular y concatenar al final: se hace seek al offset del
    bloque y se escribe ahi. Asi la memoria no depende del tamano del
    archivo. Si una replica falla, prueba la siguiente (failover).
    """
    ultimo_error = None
    c = cipher()
    for origen in ubicacion.datanodes:
        canal = _canal_datanode(origen)
        try:
            stub = dn_grpc.DataNodeServiceStub(canal)
            digest = hashlib.sha256()
            escritos = 0
            stream = stub.GetBlock(dn.GetBlockRequest(
                block_id=ubicacion.block_id,
                access_token=ubicacion.access_token))
            with open(ruta_local, "r+b") as f:
                f.seek(offset)
                for cifrado, claro in _marcos(
                        stream, ubicacion.block_id, ubicacion.size, c):
                    digest.update(cifrado)
                    f.write(claro)
                    escritos += len(claro)

            if ubicacion.sha256 and digest.hexdigest() != ubicacion.sha256:
                raise RuntimeError("checksum distinto al que registro el NameNode")
            if escritos != ubicacion.size:
                raise RuntimeError("se esperaban {} bytes y llegaron {}".format(
                    ubicacion.size, escritos))
            return escritos
        except Exception as err:
            # InvalidTag (clave equivocada o bloque alterado) tambien cae
            # aqui: se prueba la siguiente replica.
            print("  aviso: {} fallo ({})".format(origen, err or type(err).__name__))
            ultimo_error = err
        finally:
            canal.close()
    raise RuntimeError("ningun DataNode sirvio el bloque {}: {}".format(
        ubicacion.block_id, ultimo_error or "sin replicas"))


def _offsets(bloques):
    """Offset de cada bloque dentro del archivo.

    Se acumulan los tamanos reales en vez de multiplicar el indice por
    BLOCK_SIZE: asi el cliente no tiene que saber con que tamano de
    bloque decidio partir el NameNode, y el ultimo bloque parcial no
    puede descuadrar nada.
    """
    offsets = []
    acumulado = 0
    for b in bloques:
        offsets.append(acumulado)
        acumulado += b.size
    return offsets


def en_paralelo(tarea, bloques, offsets, que_hace):
    """Corre tarea(bloque, offset) sobre todos los bloques a la vez.

    Con un solo DataNode el paralelismo no compra nada. Con cuatro,
    multiplica el throughput: es lo que sostiene RNF5.
    """
    resultados = {}
    with ThreadPoolExecutor(max_workers=MAX_PARALELO) as pool:
        futuros = {pool.submit(tarea, b, o): b
                   for b, o in zip(bloques, offsets)}
        hechos = 0
        for fut in as_completed(futuros):
            b = futuros[fut]
            resultados[b.block_id] = fut.result()   # propaga la excepcion
            hechos += 1
            print("  {} {}/{}  bloque {}  {} bytes".format(
                que_hace, hechos, len(bloques), b.index, b.size))
    return resultados


class Renovador:
    """Renueva el lease mientras dura la subida (el lock de escritor).

    El NameNode da por muerto un lease que nadie renueva en LEASE_TTL
    segundos y libera el path. Una subida larga pero viva no puede
    perderlo por eso.
    """

    def __init__(self, stub, lease_id, vence, token_fn=None):
        self.stub = stub
        self.lease_id = lease_id
        self.token_fn = token_fn or token
        resto = vence - time.time() if vence else 300
        self.intervalo = max(1.0, resto / 3)
        self._fin = threading.Event()
        self._hilo = threading.Thread(target=self._correr, daemon=True)

    def _correr(self):
        while not self._fin.wait(self.intervalo):
            try:
                self.stub.Renew(nn.LeaseRequest(
                    lease_id=self.lease_id, token=self.token_fn()))
            except grpc.RpcError as err:
                print("  aviso: no se pudo renovar el lease ({})".format(
                    err.details()))

    def __enter__(self):
        self._hilo.start()
        return self

    def __exit__(self, *exc):
        self._fin.set()


def cmd_put(stub, args):
    if not os.path.isfile(args.local):
        print("no existe el archivo {}".format(args.local))
        sys.exit(1)
    size = os.path.getsize(args.local)
    remoto = ruta(args.remoto)

    asignacion = stub.Create(nn.CreateRequest(
        path=remoto, size=size, token=token()))
    bloques = sorted(asignacion.blocks, key=lambda b: b.index)
    replicas = len(bloques[0].datanodes) if bloques else 0
    print("{} -> {}   {} bytes en {} bloques x {} replicas "
          "(hasta {} a la vez)".format(
              args.local, remoto, size, len(bloques), replicas, MAX_PARALELO))

    try:
        with Renovador(stub, asignacion.lease_id, asignacion.lease_expires_at):
            respuestas = en_paralelo(
                lambda b, o: subir_bloque(b, args.local, o),
                bloques, _offsets(bloques), "subido")
        checksums = [nn.BlockChecksum(
            block_id=b.block_id, sha256=respuestas[b.block_id].sha256)
            for b in bloques]
        incompletos = [b.index for b in bloques
                       if len(respuestas[b.block_id].replicas) < len(b.datanodes)]
        if incompletos:
            print("  aviso: {} bloques con menos replicas de las pedidas; "
                  "el NameNode las repone".format(len(incompletos)))

        stub.Complete(nn.CompleteRequest(
            path=remoto, lease_id=asignacion.lease_id,
            checksums=checksums))
        print("listo: {} quedo COMMITTED".format(remoto))
    except Exception as err:
        # Si algo se rompe, cancela el lease para no dejar el path
        # bloqueado con un archivo invisible a medio subir.
        print("fallo la subida: {}".format(err))
        try:
            stub.Abort(nn.LeaseRequest(
                lease_id=asignacion.lease_id, token=token()))
            print("subida cancelada, el path quedo libre")
        except grpc.RpcError as err2:
            print("ademas fallo el Abort: {}".format(err2.details()))
        sys.exit(1)


def cmd_get(stub, args):
    remoto = ruta(args.remoto)
    info = stub.Open(nn.PathRequest(path=remoto, token=token()))
    bloques = sorted(info.blocks, key=lambda b: b.index)
    print("{} -> {}   {} bytes en {} bloques".format(
        remoto, args.local, info.size, len(bloques)))

    # Reservar el archivo completo de una vez permite que cada hilo
    # escriba en su offset sin pisar a los demas: los bloques no se
    # solapan, asi que no hace falta ningun lock. Cada hilo abre el
    # archivo por su cuenta y hace su propio seek.
    with open(args.local, "wb") as f:
        f.truncate(info.size)

    en_paralelo(lambda b, o: bajar_bloque(b, args.local, o),
                bloques, _offsets(bloques), "bajado")
    print("listo: {} ({} bytes)".format(args.local, os.path.getsize(args.local)))


HANDLERS = {
    "ping": cmd_ping,
    "login": cmd_login,
    "mkdir": cmd_mkdir,
    "ls": cmd_ls,
    "cd": cmd_cd,
    "pwd": cmd_pwd,
    "rmdir": cmd_rmdir,
    "rm": cmd_rm,
    "stat": cmd_stat,
    "chmod": cmd_chmod,
    "put": cmd_put,
    "get": cmd_get,
}


def main():
    args = build_parser().parse_args()
    stub = Lider(NAMENODES)
    try:
        HANDLERS[args.cmd](stub, args)
    except grpc.RpcError as err:
        print("error [{}]: {}".format(err.code().name, err.details()))
        sys.exit(1)
    finally:
        stub.cerrar()


if __name__ == "__main__":
    main()
