import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
output = root / "control_plane" / "rpc"
subprocess.run(
    [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        "-I",
        str(root / "proto"),
        f"--python_out={output}",
        f"--grpc_python_out={output}",
        str(root / "proto" / "engine.proto"),
    ],
    check=True,
)
path = output / "engine_pb2_grpc.py"
path.write_text(path.read_text().replace("import engine_pb2 as", "from . import engine_pb2 as"))
