#!/bin/bash

dir=$(dirname "$0")
./$dir/build_proto.sh
go run ./src/main.go "$@"