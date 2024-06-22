## About

Implementation of a map reduce system as described in [MapReduce: Simplified Data Processing on Large Clusters](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).

## Dependencies

[Protocol buffer compiler and protoc-gen-go](https://grpc.io/docs/languages/go/quickstart/)

## Running

```
# Run the master
./dev/run.sh --http.port=9000 --grpc.port=8001 --workspace-folder="./tmp"

# Run N workers
./dev/run.sh --grpc.port=8010 --master.addr=":8001" --workspace-folder="./tmp/worker_1"
```

```console
curl -X POST localhost:8002/task \
-H 'Content-Type: application/json' \
--data-binary @- << EOF
{
  "file": "./dev/input_word_count.txt",
  "numberOfPartitions": 3,
  "numberOfMapTasks": 3,
  "numberOfReduceTasks": 1,
  "scriptBase64": "$(cat word_count.js | base64 -w 0)"
}
EOF
```
