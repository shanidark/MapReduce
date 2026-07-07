# MapReduce

Distributed MapReduce system written in Go. Builds an inverted search index from a set of text files.

Storage is fully decoupled from compute: all input, intermediate spill files, and index outputs live in an S3-compatible object store (MinIO by default, but any S3 endpoint works: AWS S3, Cloudflare R2, Backblaze B2, GCS with the interoperability API, etc.). Master, workers, and client can therefore run on completely different machines, in different networks, as long as they can all reach the object store over HTTP(S).

## Build

```bash
go build
```

## Features
 * **Storage-compute separation**: input, spill files, and per-partition indexes are exchanged through S3, not through worker-to-worker gRPC or shared volumes.
 * **Let-it-crash philosophy**: worker containers use `restart: on-failure`; the master reclaims tasks from dead workers via heartbeat timeouts.
 * **Fault-tolerant via heartbeats and re-execution**: unresponsive workers are detected and their tasks are re-scheduled elsewhere. Because intermediate results are in S3, a killed worker does not lose data.
 * **Poison-pill detection**: a task that fails 3 times in a row (or reports an explicit failure like a missing input) immediately marks the job as `FAILED`.
 * **Structured logging with slog** (text or JSON, controlled by `LOG_FORMAT`).
 * **Prometheus + Grafana metrics** on ports 9091 and 3000 (Grafana creds `admin`/`admin`). Exposed metrics:
    * Currently registered workers
    * Currently alive workers
    * Total workers that have missed heartbeats
    * Total assigned tasks
    * Total completed tasks
    * Total reclaimed tasks
    * Task completion duration histogram
    * Total failed heartbeats

## Known limitations
 * **Single point of failure in the master**: if the master node dies, the whole system needs to be restarted. There is no Raft-style leader election or state replication.
 * **In-memory job state**: the jobs table lives only in the master's memory and is lost on restart.

## Running with Docker Compose

Start the cluster (master + 3 workers + MinIO + Prometheus + Grafana):

```bash
docker compose up --build
```

MinIO's S3 API is available at `localhost:9000` and its web console at `localhost:9001` (creds `minioadmin`/`minioadmin`). The `mapreduce` bucket is created automatically on first use.

`docker-compose.slow.yml` is a variant that adds `SIMULATE_SLOW_MAP` and `SIMULATE_SLOW_REDUCE` env vars to make tasks artificially slow — useful for testing fault-tolerance scenarios.

## Submitting a job

Put your input files into `./input/` on the host, then in a second terminal:

```bash
docker compose --profile manual run --rm client \
  --mode=client \
  --master_addr=master:50051 \
  /data/file1.txt /data/file2.txt
```

The client uploads each file to S3 under `input/<uuid>/<basename>`, submits the job to the master, and polls until the job finishes. On success the final inverted index is written to S3 as `index/<jobID>/final` and its key is returned in the `GetJobStatus` response.

To download the finished index locally, use `mc` or the MinIO web console.

## Usage

The single binary runs in three modes: `master`, `worker`, `client`.

### Master
```
--mode=master
--min_workers=N       # wait for at least N workers to register before dispatching (default 1)
--metrics_addr=:9090  # Prometheus metrics endpoint
```

### Worker
```
--mode=worker
--addr=<id>           # this worker's identifier (used by the master)
--master_addr=host:port
--metrics_addr=:9090
```

### Client
```
--mode=client
--master_addr=host:port
<file1> <file2> ...   # local paths to input files, uploaded to S3
```

### S3 configuration (required for all modes)

All three modes read the object-store configuration from environment variables:

| Variable         | Purpose                                        |
| ---------------- | ---------------------------------------------- |
| `S3_ENDPOINT`    | e.g. `minio:9000` or `s3.amazonaws.com`        |
| `S3_ACCESS_KEY`  | access key ID                                  |
| `S3_SECRET_KEY`  | secret key                                     |
| `S3_BUCKET`      | bucket name; created on startup if missing     |
| `S3_USE_SSL`     | `"true"` for HTTPS endpoints, otherwise `"false"` |

### Logging
Structured logging via `slog`. Set `LOG_FORMAT=json` for JSON output (recommended in production), any other value produces human-readable text.

## Object layout in S3

For a job with ID `J` and `P` reduce partitions:

```
input/<upload-uuid>/<basename>       # uploaded by the client
spill/J/<mapTaskID>/<partitionID>    # written by map workers
index/J/<partitionID>                # written by reduce workers
index/J/final                        # written by the master after all reduces finish
```

The reduce phase does not need to know which worker produced a given spill: reducers simply `List` the prefix `spill/J/` and filter by partition suffix.

## Testing

Unit tests:
```bash
go test ./...
```

Integration tests (spin up the full docker-compose cluster and verify happy path + several worker-kill scenarios):
```bash
go test -tags integration -v -timeout 20m
```

The integration suite talks to Docker directly, so if your user isn't in the `docker` group prepend `sudo -E`.
