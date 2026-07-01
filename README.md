# MapReduce
## MapReduce distributed system written in Golang.
Builds an inverted search index based on given text files.
Currently indexes only by one word. (n-gramms planned, WIP)

## Build
Builds simply with `go build`. 

## Features
 * Heartbeats & re-execution
 * Structured logging with slog

## Running with Docker Compose
Build and run 1 master instance + 3 workers on a shared network:
```bash
docker compose up --build
```

## Usage
Can be launched in 2 modes: `master` & `worker`. Files that are meant to be indexed must be on master machine.
Structured logging can be done in 2 formats: textual and JSON. By default writes in textual format to stdout, in production-like environments set `LOG_FORMAT=JSON` to output structured logs to stdout
### Master
To launch in master mode use `--mode=master`, also there is `--min_workers=n` (default 1) to make master machine wait for at least n workers to connect before sending tasks.
### Worker
To launch in worker mode use `--mode=worker`, `--addr=` to set current worker machine address (with port) and `--master_addr=` to set which master machine to connect.


Logs are pretty informative, reporting that a worker has connected to the master and vice-versa, sending and requesting tasks and their statuses.

Final results will be in the `index` file on master machine
