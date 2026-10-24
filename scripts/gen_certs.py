"""Genera la CA del cluster y los certificados para mTLS.

Es lo mismo que hace el taller de nginx con openssl
(openssl req -x509 -nodes -days 365 -newkey rsa:2048 ...), pero en
Python para que corra igual en Windows, donde openssl no viene
instalado. El resultado:

    certs/ca.crt  ca.key          la autoridad del cluster
    certs/nodo.crt  nodo.key      NameNodes y DataNodes
    certs/cliente.crt cliente.key la CLI

    python scripts/gen_certs.py
    python scripts/gen_certs.py --carpeta /etc/dfsha/certs --dias 90

ca.key NO se copia a las maquinas: solo se usa para firmar. A cada
maquina van ca.crt y el par de su rol.
"""

import argparse
import datetime
import os

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOMBRE_TLS = "dfsha"     # el mismo de common/seguridad.py


def _clave():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _guardar(carpeta, nombre, clave, cert):
    with open(os.path.join(carpeta, nombre + ".key"), "wb") as f:
        f.write(clave.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption()))
    with open(os.path.join(carpeta, nombre + ".crt"), "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    try:
        os.chmod(os.path.join(carpeta, nombre + ".key"), 0o600)
    except OSError:
        pass


def generar(carpeta, dias=365):
    os.makedirs(carpeta, exist_ok=True)
    ahora = datetime.datetime.now(datetime.timezone.utc)
    vence = ahora + datetime.timedelta(days=dias)

    ca_clave = _clave()
    ca_nombre = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "dfsha-ca")])
    ca_cert = (x509.CertificateBuilder()
               .subject_name(ca_nombre).issuer_name(ca_nombre)
               .public_key(ca_clave.public_key())
               .serial_number(x509.random_serial_number())
               .not_valid_before(ahora).not_valid_after(vence)
               .add_extension(x509.BasicConstraints(ca=True, path_length=0),
                              critical=True)
               .sign(ca_clave, hashes.SHA256()))
    _guardar(carpeta, "ca", ca_clave, ca_cert)

    for rol in ("nodo", "cliente"):
        clave = _clave()
        cert = (x509.CertificateBuilder()
                .subject_name(x509.Name([
                    x509.NameAttribute(NameOID.COMMON_NAME, rol)]))
                .issuer_name(ca_nombre)
                .public_key(clave.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(ahora).not_valid_after(vence)
                .add_extension(x509.SubjectAlternativeName(
                    [x509.DNSName(NOMBRE_TLS), x509.DNSName("localhost")]),
                    critical=False)
                # Los nodos son servidores Y clientes (el DataNode late
                # hacia el NameNode y reenvia por el pipeline).
                .add_extension(x509.ExtendedKeyUsage([
                    ExtendedKeyUsageOID.SERVER_AUTH,
                    ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
                .sign(ca_clave, hashes.SHA256()))
        _guardar(carpeta, rol, clave, cert)
    return carpeta


def main():
    ap = argparse.ArgumentParser(prog="gen_certs")
    ap.add_argument("--carpeta", default=os.path.join(RAIZ, "certs"))
    ap.add_argument("--dias", type=int, default=365)
    args = ap.parse_args()
    generar(args.carpeta, args.dias)
    print("Certificados en {}:".format(args.carpeta))
    for f in sorted(os.listdir(args.carpeta)):
        print("  " + f)
    print("ca.key no se copia a ninguna maquina: solo sirve para firmar.")


if __name__ == "__main__":
    main()
