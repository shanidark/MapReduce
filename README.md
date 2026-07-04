# MapReduce
## MapReduce distributed system written in Golang.
Builds an inverted search index based on given text files.
Currently indexes only by one word. (n-gramms planned, WIP)

## Build
Builds simply with `go build`. 

## Features
 * Heartbeats & re-execution
 * Structured logging with slog
 * Grafana and Prometheus metrics logging. Built-in metrics are:
    * Currently registered workers
    * Currently alive workers
    * Total amount of workers that've missed heartbeats
    * Total amount of assigned tasks
    * Total amount of done tasks
    * Total amount of reclaimed tasks (after a worker had missed its heartbeat)
    * Task completion duration
    * Total number of failed heartbeats (error returned)

## Running with Docker Compose
Build and run 1 master instance + 3 workers on a shared network:
```bash
docker compose up --build
```
To better test fault-tolerance on not so big data env variables `SIMULATE_SLOW_MAP` & `SIMULATE_SLOW_REDUCE` can be used to slow down respective phases by N seconds.

## Usage
Can be launched in 2 modes: `master` & `worker`. Files that are meant to be indexed must be on master machine.
Structured logging can be done in 2 formats: textual and JSON. By default writes in textual format to stdout, in production-like environments set `LOG_FORMAT=JSON` to output structured logs to stdout
### Master
To launch in master mode use `--mode=master`, also there is `--min_workers=n` (default 1) to make master machine wait for at least n workers to connect before sending tasks.
### Worker
To launch in worker mode use `--mode=worker`, `--addr=` to set current worker machine address (with port) and `--master_addr=` to set which master machine to connect.

## Testing
Use `sudo go test -tags integration -v -timeout 10m` to run tests including fault-tolerance ones


Logs are pretty informative, reporting that a worker has connected to the master and vice-versa, sending and requesting tasks and their statuses.

Final results will be in the `output/index` file on master machine
