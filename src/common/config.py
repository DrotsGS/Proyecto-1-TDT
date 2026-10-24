"""Configuracion por variables de entorno.

Nunca escribas una IP o un puerto dentro del codigo. Este patron es el
que hace que el mismo codigo corra sin cambios en Windows, en Docker y
en EC2 (es el REMOTE_HOST del laboratorio de la calculadora).
"""

import os

# Raiz del repositorio (este archivo esta en src/common/).
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def token_file() -> str:
    """Donde el cliente guarda el token del ultimo login."""
    return os.environ.get(
        "DFSHA_TOKEN_FILE",
        os.path.join(os.path.expanduser("~"), ".dfsha_token"))


def read_token() -> str:
    """Token de la variable de entorno, o el del ultimo login.

    La variable gana: asi se puede correr como otro usuario sin borrar
    el archivo.
    """
    desde_env = os.environ.get("DFSHA_TOKEN")
    if desde_env:
        return desde_env
    try:
        with open(token_file(), encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        return ""


def cwd_file() -> str:
    """Donde el cliente recuerda el directorio actual (el cd de RF1)."""
    return os.environ.get(
        "DFSHA_CWD_FILE",
        os.path.join(os.path.expanduser("~"), ".dfsha_cwd"))


def read_cwd() -> str:
    try:
        with open(cwd_file(), encoding="utf-8") as f:
            return f.read().strip() or "/"
    except OSError:
        return "/"


def write_cwd(path: str) -> None:
    with open(cwd_file(), "w", encoding="utf-8") as f:
        f.write(path)


def data_dir(node_id: str) -> str:
    """Carpeta de bloques del DataNode.

    En Docker apunta a /data (un volumen por nodo).
    En Windows cae en <repo>/data/<node_id>, para que dos DataNodes
    locales no se pisen los bloques.
    """
    d = os.environ.get("DATA_DIR")
    if d:
        return d
    return os.path.join(ROOT, "data", node_id)
