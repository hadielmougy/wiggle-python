#!/usr/bin/env bash
#
# Regenerates the gRPC stubs in wiggle/_proto/ from the vendored proto (proto/wiggle.proto).
# The proto is the wire contract with the Wiggle server; keep proto/wiggle.proto in sync with the
# canonical copy in the server repo (wiggle: proto/src/main/proto/wiggle.proto) and re-run this.
#
#   pip install grpcio-tools     # (or: pip install -e '.[dev]')
#   ./codegen.sh
set -euo pipefail
cd "$(dirname "$0")"

PROTO_DIR="proto"
OUT="wiggle/_proto"

python3 -m grpc_tools.protoc \
    -I "$PROTO_DIR" \
    --python_out="$OUT" \
    --grpc_python_out="$OUT" \
    "$PROTO_DIR/wiggle.proto" "$PROTO_DIR/coordinator.proto"

# generated stubs import sibling '*_pb2' modules absolutely; make them relative so they work as a
# package (covers wiggle_pb2, coordinator_pb2, and the grpc modules importing each other).
perl -pi -e 's/^import (\w+_pb2)( as|\b)/from . import $1$2/' "$OUT"/*_pb2.py "$OUT"/*_pb2_grpc.py
: > "$OUT/__init__.py"
echo "regenerated stubs in $OUT"
