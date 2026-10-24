"""
SDK de DFSha: RF3, acceso a archivos.

La CLI cubre RF1 (gestion) y RF2 (transferencia: put y get de archivos
enteros). Este modulo cubre RF3: abrir un archivo remoto y leerlo o
escribirlo por partes, como un archivo local.

    from client.sdk import DFSha

    fs = DFSha(["nn1:50051", "nn2:50051", "nn3:50051"])
    fs.login("drots", "dfsha")

    with fs.open("/docs/notas.txt", "w") as f:     # open + lock
        f.write(b"hola ")
        f.write(b"mundo")                           # write
    # close: commit, el archivo se vuelve visible e inmutable

    with fs.open("/docs/notas.txt") as f:           # open
        f.seek(5)
        print(f.read(5))                            # read -> b"mundo"

Semantica (D2, WORM): un archivo se escribe UNA vez, de principio a fin,
y despues solo se lee. Por eso:

  - open(path, "w") toma el lock: el lease de escritor unico. Mientras
    este abierto nadie mas puede crear ese path (ALREADY_EXISTS), y el
    archivo es invisible para ls. lock() devuelve ese lease.
  - write() solo agrega al final. No hay escritura aleatoria.
  - close() hace el commit. Si el proceso muere antes, el lease vence y
    el NameNode libera el path (LEASE_TTL).
  - read() y seek() son de acceso aleatorio: solo se baja el bloque que
    contiene el offset pedido, no el archivo entero.
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pb import comun, nn                     # noqa: E402
from client import cli                              # noqa: E402


class DFSha:
    """Una sesion contra el cluster. Todas las rutas son absolutas."""

    def __init__(self, namenodes=None, token=None):
        self.stub = cli.Lider(namenodes or cli.NAMENODES)
        self.token = token or ""

    def _t(self):
        return self.token

    def login(self, usuario, password):
        self.token = self.stub.Login(nn.LoginRequest(
            user=usuario, password=password)).token
        return self.token

    # ---------------- RF1 ----------------

    def ls(self, path="/"):
        return list(self.stub.Ls(nn.PathRequest(path=path, token=self.token)).entries)

    def mkdir(self, path):
        return self.stub.Mkdir(nn.PathRequest(path=path, token=self.token)).ok

    def rmdir(self, path):
        return self.stub.Rmdir(nn.PathRequest(path=path, token=self.token)).ok

    def rm(self, path):
        return self.stub.Rm(nn.PathRequest(path=path, token=self.token)).ok

    def stat(self, path):
        return self.stub.Stat(nn.PathRequest(path=path, token=self.token))

    def chmod(self, path, modo, grupo=""):
        return self.stub.Chmod(nn.ChmodRequest(
            path=path, mode=modo, group=grupo, token=self.token)).ok

    # ---------------- RF2, sobre RF3 ----------------
    # El enunciado lo dice: send() y receive() se pueden implementar con
    # las primitivas de acceso. Aqui se hace asi a proposito.

    def send(self, local, remoto):
        with open(local, "rb") as origen, self.open(remoto, "w") as destino:
            for trozo in iter(lambda: origen.read(cli.CHUNK_SIZE), b""):
                destino.write(trozo)

    def receive(self, remoto, local):
        with self.open(remoto) as origen, open(local, "wb") as destino:
            for trozo in iter(lambda: origen.read(cli.CHUNK_SIZE), b""):
                destino.write(trozo)

    # ---------------- RF3 ----------------

    def open(self, path, modo="r"):
        if modo in ("r", "rb"):
            return ArchivoLectura(self, path)
        if modo in ("w", "wb"):
            return ArchivoEscritura(self, path)
        raise ValueError("modo no soportado (WORM): {}".format(modo))

    def cerrar(self):
        self.stub.cerrar()


class ArchivoEscritura:
    """write() de RF3. Escribe bloque a bloque, sin saber el tamano final.

    Los bytes se juntan en un archivo temporal local hasta completar un
    bloque; entonces se pide el bloque al NameNode (AddBlock) y se sube
    por el pipeline como cualquier otro. Nunca hay mas de un bloque en
    el temporal.
    """

    def __init__(self, fs, path):
        self.fs = fs
        self.path = path
        r = fs.stub.Create(nn.CreateRequest(path=path, size=0, token=fs.token))
        self.lease_id = r.lease_id
        self.block_size = r.block_size
        self._renovador = cli.Renovador(fs.stub, r.lease_id,
                                        r.lease_expires_at, fs._t).__enter__()
        fd, self._temporal = tempfile.mkstemp(prefix="dfsha-w-")
        self._buffer = os.fdopen(fd, "w+b")
        self._en_buffer = 0
        self._checksums = []
        self.cerrado = False

    def lock(self):
        """El lock de escritor unico ya se tomo en el open: es el lease."""
        return self.lease_id

    def write(self, datos):
        if self.cerrado:
            raise ValueError("el archivo ya esta cerrado")
        vista = memoryview(datos)
        while len(vista):
            cabe = self.block_size - self._en_buffer
            parte = vista[:cabe]
            self._buffer.write(parte)
            self._en_buffer += len(parte)
            vista = vista[len(parte):]
            if self._en_buffer == self.block_size:
                self._subir()
        return len(datos)

    def _subir(self):
        if self._en_buffer == 0:
            return
        self._buffer.flush()
        asignacion = self.fs.stub.AddBlock(nn.AddBlockRequest(
            lease_id=self.lease_id, size=self._en_buffer, token=self.fs.token))
        r = cli.subir_bloque(asignacion, self._temporal, 0)
        self._checksums.append(nn.BlockChecksum(
            block_id=asignacion.block_id, sha256=r.sha256))
        self._buffer.seek(0)
        self._buffer.truncate()
        self._en_buffer = 0

    def close(self):
        if self.cerrado:
            return
        try:
            self._subir()
            self.fs.stub.Complete(nn.CompleteRequest(
                path=self.path, lease_id=self.lease_id,
                checksums=self._checksums))
        finally:
            self.cerrado = True
            self._renovador.__exit__(None, None, None)
            self._buffer.close()
            os.remove(self._temporal)

    def abort(self):
        if self.cerrado:
            return
        self.cerrado = True
        self._renovador.__exit__(None, None, None)
        self._buffer.close()
        os.remove(self._temporal)
        self.fs.stub.Abort(nn.LeaseRequest(
            lease_id=self.lease_id, token=self.fs.token))

    def __enter__(self):
        return self

    def __exit__(self, tipo, *_):
        # Si hubo una excepcion adentro del with, no se hace commit de un
        # archivo a medias: se cancela y el path queda libre.
        if tipo is None:
            self.close()
        else:
            self.abort()


class ArchivoLectura:
    """read() y seek() de RF3, con acceso aleatorio por bloque."""

    def __init__(self, fs, path):
        self.fs = fs
        self.path = path
        info = fs.stub.Open(nn.PathRequest(path=path, token=fs.token))
        self.size = info.size
        self._bloques = sorted(info.blocks, key=lambda b: b.index)
        self._inicios = cli._offsets(self._bloques)
        self._pos = 0
        self._cache_indice = None
        fd, self._temporal = tempfile.mkstemp(prefix="dfsha-r-")
        os.close(fd)
        self.cerrado = False

    def tell(self):
        return self._pos

    def seek(self, offset, desde=0):
        base = {0: 0, 1: self._pos, 2: self.size}[desde]
        self._pos = max(0, base + offset)
        return self._pos

    def _bloque_de(self, pos):
        for i in range(len(self._bloques) - 1, -1, -1):
            if self._inicios[i] <= pos:
                return i
        return 0

    def _cargar(self, i):
        """Baja SOLO el bloque i (con failover entre sus replicas)."""
        if self._cache_indice == i:
            return
        bloque = self._bloques[i]
        with open(self._temporal, "wb") as f:
            f.truncate(bloque.size)
        cli.bajar_bloque(bloque, self._temporal, 0)
        self._cache_indice = i

    def read(self, n=-1):
        if self.cerrado:
            raise ValueError("el archivo ya esta cerrado")
        if n is None or n < 0:
            n = self.size - self._pos
        salida = bytearray()
        while n > 0 and self._pos < self.size:
            i = self._bloque_de(self._pos)
            self._cargar(i)
            dentro = self._pos - self._inicios[i]
            cuantos = min(n, self._bloques[i].size - dentro)
            with open(self._temporal, "rb") as f:
                f.seek(dentro)
                trozo = f.read(cuantos)
            salida.extend(trozo)
            self._pos += len(trozo)
            n -= len(trozo)
        return bytes(salida)

    def close(self):
        if not self.cerrado:
            self.cerrado = True
            os.remove(self._temporal)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


__all__ = ["DFSha", "ArchivoLectura", "ArchivoEscritura", "comun"]
