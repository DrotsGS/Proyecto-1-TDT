"""
Genera los stubs gRPC a partir de los .proto de src/proto/.

Multiplataforma: funciona igual en Windows, Linux y dentro de Docker.
Reemplaza al 'make proto' de los laboratorios, que dependia de sed.

    python scripts/gen_proto.py
"""

import os
import sys
import subprocess

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROTO_DIR = os.path.join(ROOT, "src", "proto")
OUT_DIR = os.path.join(PROTO_DIR, "gen")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # Los cuatro archivos de una vez: el -I resuelve los imports entre
    # ellos (los tres servicios importan dfsha_common.proto).
    protos = sorted(f for f in os.listdir(PROTO_DIR) if f.endswith(".proto"))
    if not protos:
        print("No hay archivos .proto en {}".format(PROTO_DIR))
        sys.exit(1)

    cmd = [
        sys.executable, "-m", "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={OUT_DIR}",
        f"--pyi_out={OUT_DIR}",
        f"--grpc_python_out={OUT_DIR}",
    ] + [os.path.join(PROTO_DIR, p) for p in protos]

    print("Generando stubs de: {}".format(", ".join(protos)))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print("\nFallo la generacion. Revisa la sintaxis de los .proto.")
        sys.exit(1)

    # Marca src/proto/gen como paquete importable
    init = os.path.join(OUT_DIR, "__init__.py")
    if not os.path.exists(init):
        open(init, "w").close()

    print(f"Listo. Stubs en {OUT_DIR}")
    for f in sorted(os.listdir(OUT_DIR)):
        if f.endswith((".py", ".pyi")):
            print(f"  {f}")


if __name__ == "__main__":
    main()
