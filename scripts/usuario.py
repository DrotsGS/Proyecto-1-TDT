"""Alta y cambio de contrasena de usuarios de DFSha.

El archivo guarda el sha256 de la contrasena, nunca la contrasena.

    python scripts/usuario.py drots micontrasena
    python scripts/usuario.py --archivo deploy/usuarios.json profesor otra
"""

import argparse
import hashlib
import json
import os

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
POR_DEFECTO = os.path.join(RAIZ, "deploy", "usuarios.json")


def main():
    ap = argparse.ArgumentParser(prog="usuario")
    ap.add_argument("usuario")
    ap.add_argument("password")
    ap.add_argument("--archivo", default=POR_DEFECTO)
    args = ap.parse_args()

    usuarios = {}
    if os.path.exists(args.archivo):
        with open(args.archivo, encoding="utf-8") as f:
            usuarios = json.load(f)

    usuarios[args.usuario] = hashlib.sha256(args.password.encode()).hexdigest()

    os.makedirs(os.path.dirname(args.archivo), exist_ok=True)
    with open(args.archivo, "w", encoding="utf-8") as f:
        json.dump(usuarios, f, indent=2, sort_keys=True)
        f.write("\n")
    print("{} guardado en {}".format(args.usuario, args.archivo))


if __name__ == "__main__":
    main()
