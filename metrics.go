package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// master
	workersRegistered = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mr_workers_registered",
		Help: "Number of currently registered workers.",
	})

	workersTimedOut = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mr_workers_timed_out_total",
		Help: "Total number of workers that missed heartbeat and got dropped.",
	})

	tasksAssigned = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mr_tasks_assigned_total",
		Help: "Total number of tasks handed out to workers.",
	}, []string{"phase"})

	tasksCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mr_tasks_completed_total",
		Help: "Total number of tasks reported as done.",
	}, []string{"phase"})

	tasksReclaimed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mr_tasks_reclaimed_total",
		Help: "Total number of tasks reset back to idle after a worker was lost.",
	}, []string{"phase"})

	// worker
	taskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mr_task_duration_seconds",
		Help:    "Duration of a task from start to done.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	}, []string{"phase"})

	heartbeatsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mr_heartbeats_failed_total",
		Help: "Total number of heartbeat RPCs that returned an error.",
	})
	
	workerUp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mr_worker_up",
		Help: "1 if the worker is currently registered and heartbeating, 0 if it timed out.",
	}, []string{"worker_addr"})
)

func serveMetrics(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(addr, mux); err != nil {
		panic(err)
	}
}
