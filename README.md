# MapReduce
## MapReduce distributed system written in Golang.
Builds an inverted search index based on given text files.
## Build
Builds simply with `go build`. 

## Features
 * let-it-crash philosophy, docker files utilize restart on failure policy
 * Fault-tolerant via heartbeats & re-execution. Failures caused by non-existent files are handled by instantly marking job as failed
 * Structured logging with slog
 * Grafana and Prometheus metrics logging. 
    * Ports are 3000 and 9091 respectively. Grafana creds are admin/admin
    * Built-in metrics are:
        * Currently registered workers
        * Currently alive workers
        * Total amount of workers that've missed heartbeats
        * Total amount of assigned tasks
        * Total amount of done tasks
        * Total amount of reclaimed tasks (after a worker had missed its heartbeat)
        * Task completion duration
        * Total number of failed heartbeats (error returned)

## Known limitations
 * Single point of failure in master: if master node fails, the whole system fails and needs to be restarted
 * In-memory state is lost on restart

## Running with Docker Compose
Build and run 1 master instance + 3 workers on a shared network:
```bash
docker compose up --build
```
`docker-compose.slow.yml` contains environment variables to slow down work

## Usage
Can be launched in 3 modes: `master`, `worker`, `client`. Input files must be accessible at the same path on all worker machines.
In the docker compose setup this is achieved by mounting a shared volume to all containers. Final results will be in the `output/` directory 
For a real distributed deployment use an S3-compatible storage instead.
Structured logging can be done in 2 formats: textual and JSON. By default writes in textual format to stdout, in production-like environments set `LOG_FORMAT=JSON` to output structured logs to stdout
### Master
To launch in master mode use `--mode=master`, also there is `--min_workers=n` (default 1) to make master machine wait for at least n workers to connect before sending tasks.
### Worker
To launch in worker mode use `--mode=worker`, `--addr=` to set current worker machine address (with port) and `--master_addr=` to set which master machine to connect.
### Client
To launch in client mode use `--mode=client`, `--master_addr=` and space-separated input files

## Submitting a job
Once the computation cluster is set up run a client instance following above instructions. The final index will be written to `output/index-{job_id}` on the master. Submit new jobs by putting files into `input/`

## Testing
`sudo go test` if the user isn't in docker group
