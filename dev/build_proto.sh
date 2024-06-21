#!/bin/bash

echo "Compiling .proto files"

# Generate Go files from .proto files.
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    src/proto/core.proto
