## About

Implementation of a map reduce system as described in [MapReduce: Simplified Data Processing on Large Clusters](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).

## Dependencies

[Protocol buffer compiler and protoc-gen-go](https://grpc.io/docs/languages/go/quickstart/)

## Running

```
curl localhost:8002/task --data "$(cat word_count.js)"
```
