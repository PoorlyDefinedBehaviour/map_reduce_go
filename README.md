## About

Implementation of a map reduce system as described in [MapReduce: Simplified Data Processing on Large Clusters](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).

## Dependencies

[Protocol buffer compiler and protoc-gen-go](https://grpc.io/docs/languages/go/quickstart/)

## Running

```console
curl -X POST localhost:8002/task \
-H 'Content-Type: application/json' \
--data-binary @- << EOF
{
  "file": "./dev/input_word_count.txt",
  "folder": "/tmp",
  "numberOfPartitions": 3,
  "numberOfMapTasks": 3,
  "numberOfReduceTasks": 1,
  "scriptBase64": "$(cat word_count.js | base64 -w 0)"
}
EOF
```
