"""
Punto unico de importacion de los stubs generados.

El codigo que genera protoc usa imports planos ('import dfsha_pb2'), que
no funcionan si importas proto.gen como paquete. En vez de reescribir el
archivo generado con sed (que no existe en Windows), agregamos proto/gen
al sys.path y lo importamos plano desde aqui.

Desde la semana 10 el contrato vive en cuatro archivos, uno por par de
comunicacion, asi que aqui hay cuatro modulos con alias cortos:

    from common.pb import comun, nn, nn_grpc, dn, dn_grpc, ctl, ctl_grpc

    comun   tipos compartidos      (Empty, StatusResponse, Entry, FileState)
    nn      cliente <-> NameNode   (plano de control)
    dn      cliente <-> DataNode   (plano de datos)
    ctl     NameNode <-> DataNode  (heartbeat y comandos piggyback)
"""

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_GEN = os.path.join(_ROOT, "proto", "gen")

if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
if _GEN not in sys.path:
    sys.path.insert(0, _GEN)

try:
    import dfsha_common_pb2 as comun
    import dfsha_namenode_pb2 as nn
    import dfsha_namenode_pb2_grpc as nn_grpc
    import dfsha_datanode_pb2 as dn
    import dfsha_datanode_pb2_grpc as dn_grpc
    import dfsha_control_pb2 as ctl
    import dfsha_control_pb2_grpc as ctl_grpc
except ImportError as e:
    raise ImportError(
        "No se encontraron los stubs de gRPC.\n"
        "Ejecuta primero:  python scripts/gen_proto.py"
    ) from e

__all__ = ["comun", "nn", "nn_grpc", "dn", "dn_grpc", "ctl", "ctl_grpc"]
