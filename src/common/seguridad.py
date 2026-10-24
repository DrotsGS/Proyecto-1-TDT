"""Canales y puertos gRPC, con o sin mTLS.

Semana 12. Todo el proyecto abre canales y puertos por aqui, nunca con
grpc.insecure_channel directo. Asi se desarrolla sin certificados y se
despliega con ellos cambiando UNA variable:

    DFSHA_TLS=1          activa mTLS en todos los pares de comunicacion
    DFSHA_CERTS=certs    carpeta con ca.crt y los pares <rol>.crt/.key
    DFSHA_CERT=nodo      que certificado usa este proceso
                         (nodo para NameNode y DataNode, cliente para la CLI)

mTLS = las DOS puntas presentan certificado firmado por la CA propia
del cluster. Un DataNode no acepta bloques de quien no tenga uno, y un
NameNode no acepta heartbeats de un DataNode impostor. Es la "seguridad
entre nodos de datos y control" que pide RNF6.

Los certificados se generan con scripts/gen_certs.py (o con openssl,
como en el taller de nginx: ver docs/despliegue.md).

Sobre el nombre del servidor: los nodos se alcanzan por localhost, por
nombre de servicio en Docker o por IP privada en AWS. En vez de emitir
un certificado por cada direccion posible, todos llevan el nombre
"dfsha" y el canal lo verifica contra ese nombre. La identidad que se
comprueba es "firmado por la CA del cluster", que es lo que importa
dentro del cluster.
"""

import os

import grpc

from common.config import ROOT

NOMBRE_TLS = "dfsha"

# Los chunks de 1 MB cifrados pesan 1 MB + 28 bytes; el limite por
# defecto de gRPC (4 MB) alcanza, pero se sube para no depender de eso.
_OPCIONES = [
    ("grpc.max_send_message_length", 16 * 1024 * 1024),
    ("grpc.max_receive_message_length", 16 * 1024 * 1024),
]


def tls_activo():
    return os.environ.get("DFSHA_TLS", "0") == "1"


def _carpeta():
    return os.environ.get("DFSHA_CERTS") or os.path.join(ROOT, "certs")


def _leer(nombre):
    with open(os.path.join(_carpeta(), nombre), "rb") as f:
        return f.read()


def _par(rol):
    rol = rol or os.environ.get("DFSHA_CERT", "nodo")
    return _leer(rol + ".key"), _leer(rol + ".crt")


def canal(direccion, rol=None):
    """Canal hacia direccion. Con TLS, presenta el certificado de rol."""
    if not tls_activo():
        return grpc.insecure_channel(direccion, options=_OPCIONES)
    clave, cert = _par(rol)
    credenciales = grpc.ssl_channel_credentials(
        root_certificates=_leer("ca.crt"),
        private_key=clave, certificate_chain=cert)
    return grpc.secure_channel(
        direccion, credenciales,
        options=_OPCIONES + [("grpc.ssl_target_name_override", NOMBRE_TLS)])


def servidor(executor):
    return grpc.server(executor, options=_OPCIONES)


def abrir_puerto(server, direccion, rol=None):
    """add_insecure_port o add_secure_port, segun DFSHA_TLS.

    require_client_auth=True es lo que lo vuelve MUTUO: sin certificado
    del cliente, el handshake ni siquiera termina.
    """
    if not tls_activo():
        return server.add_insecure_port(direccion)
    clave, cert = _par(rol)
    credenciales = grpc.ssl_server_credentials(
        [(clave, cert)], root_certificates=_leer("ca.crt"),
        require_client_auth=True)
    return server.add_secure_port(direccion, credenciales)
