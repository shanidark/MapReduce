package main

import (
	"bufio"
	"context"
	"log/slog"
	pb "mapreduce/proto"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

const maxTaskAttempts = 3

func (m *masterImpl) RegisterWorker(_ context.Context,
	info *pb.WorkerInfo) (*pb.Ack, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for _, addr := range m.workers {
		if addr == info.Addr {
			m.lastSeen[info.Addr] = time.Now()
			slog.Info("worker registered again (already known)", "worker_addr", info.Addr)
			return &pb.Ack{}, nil
		}
	}

	m.workers = append(m.workers, info.Addr)
	m.lastSeen[info.Addr] = time.Now()
	workersRegistered.Set(float64(len(m.workers)))
	workerUp.WithLabelValues(info.Addr).Set(1)
	slog.Info("registered worker", "worker_addr", info.Addr, "total_workers", len(m.workers))
	return &pb.Ack{}, nil
}

func (m *masterImpl) RequestTask(_ context.Context,
	req *pb.TaskRequest) (*pb.Task, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if len(m.workers) >= m.minWorkers {
		m.started = true
	}

	if !m.started {
		return &pb.Task{Type: pb.Task_IDLE}, nil
	}

	// crawling jobs
	for _, jobID := range sortedJobIDs(m.jobs) {
		j := m.jobs[jobID]
		if j.status != jobRunning {
			continue
		}

		// assigning map tasks
		for i := range j.mapTasks {
			task := &j.mapTasks[i]
			if task.state == taskIdle {
				task.attempts++
				if task.attempts > maxTaskAttempts {
					task.state = taskFailed
					slog.Error("map task failed after max attempts",
						"job_id", jobID, "task_id", task.id, "file", task.filePath)
					continue
				}
				task.state = taskRunning
				task.workerAddr = req.WorkerAddr
				slog.Info("given task", "worker_addr", req.WorkerAddr, "task_type", pb.Task_MAP, "task_id", int32(task.id), "job_id", jobID)
				tasksAssigned.WithLabelValues("map").Inc()
				return &pb.Task{
					Type:          pb.Task_MAP,
					TaskId:        int32(task.id),
					FilePath:      task.filePath,
					NumPartitions: int32(j.numPartitions),
					JobId:         jobID,
				}, nil
			}
		}

		// dont assign reduces before completing all maps
		if j.mapDone < len(j.mapTasks) {
			continue
		}

		for i := range j.reduceTasks {
			task := &j.reduceTasks[i]
			if task.state == taskIdle {
				task.attempts++
				if task.attempts > maxTaskAttempts {
					task.state = taskFailed
					slog.Error("reduce task failed after max attempts",
						"job_id", jobID, "task_id", task.id)
					continue
				}
				task.state = taskRunning
				task.workerAddr = req.WorkerAddr
				slog.Info("given task", "worker_addr", req.WorkerAddr, "task_type", pb.Task_REDUCE, "task_id", int32(task.id), "job_id", jobID)
				tasksAssigned.WithLabelValues("reduce").Inc()
				return &pb.Task{
					Type:       pb.Task_REDUCE,
					TaskId:     int32(task.id),
					Partition:  int32(task.partition),
					SpillAddrs: m.workers,
					JobId:      jobID,
				}, nil
			}
		}
		if hasFailedTasks(j) && j.status == jobRunning {
			j.status = jobFailed
			slog.Warn("job marked as failed", "job_id", jobID)
		}
	}

	return &pb.Task{Type: pb.Task_IDLE}, nil
}

func (m *masterImpl) ReportDone(_ context.Context,
	done *pb.TaskDone) (*pb.Ack, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	j, ok := m.jobs[done.JobId]
	if !ok {
		return &pb.Ack{}, nil
	}

	// worker reported explicit failure — mark task failed and fail the job
	if done.Failed {
		for i := range j.mapTasks {
			task := &j.mapTasks[i]
			if task.id == int(done.TaskId) && task.state == taskRunning {
				task.state = taskFailed
				slog.Error("map task reported failure",
					"job_id", j.id, "task_id", task.id, "err", done.ErrorMessage)
				if j.status == jobRunning {
					j.status = jobFailed
					slog.Warn("job marked as failed", "job_id", j.id)
				}
				return &pb.Ack{}, nil
			}
		}
		for i := range j.reduceTasks {
			task := &j.reduceTasks[i]
			if task.id == int(done.TaskId) && task.state == taskRunning {
				task.state = taskFailed
				slog.Error("reduce task reported failure",
					"job_id", j.id, "task_id", task.id, "err", done.ErrorMessage)
				if j.status == jobRunning {
					j.status = jobFailed
					slog.Warn("job marked as failed", "job_id", j.id)
				}
				return &pb.Ack{}, nil
			}
		}
		return &pb.Ack{}, nil
	}

	// success path
	for i := range j.mapTasks {
		task := &j.mapTasks[i]
		if task.id == int(done.TaskId) && task.state == taskRunning {
			task.state = taskDone
			j.mapDone++
			tasksCompleted.WithLabelValues("map").Inc()
			return &pb.Ack{}, nil
		}
	}
	for i := range j.reduceTasks {
		task := &j.reduceTasks[i]
		if task.id == int(done.TaskId) && task.state == taskRunning {
			task.state = taskDone
			task.workerAddr = done.WorkerAddr
			j.reduceDone++
			tasksCompleted.WithLabelValues("reduce").Inc()

			if j.reduceDone == len(j.reduceTasks) {
				j.status = jobCollecting
				go m.finalizeJob(j) // building final index after a job has been done
			}
			return &pb.Ack{}, nil
		}
	}
	return &pb.Ack{}, nil
}

func (m *masterImpl) Heartbeat(_ context.Context,
	req *pb.HeartbeatRequest) (*pb.Ack, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.lastSeen[req.WorkerAddr] = time.Now()
	return &pb.Ack{}, nil
}

func (m *masterImpl) checkTimeouts(timeout time.Duration) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	now := time.Now()
	for addr, last := range m.lastSeen {
		if now.Sub(last) <= timeout {
			continue
		}
		slog.Warn("worker time out", "worker_addr", addr, "last_seen", last)
		workersTimedOut.Inc()
		workerUp.WithLabelValues(addr).Set(0)

		// reclaim all tasks of the dead worker across all active jobs
		for _, j := range m.jobs {
			if j.status != jobRunning {
				continue
			}
			for i := range j.mapTasks {
				task := &j.mapTasks[i]
				if task.workerAddr == addr && task.state != taskIdle && task.state != taskFailed {
					if task.state == taskDone {
						j.mapDone--
					}
					if task.attempts >= maxTaskAttempts {
						task.state = taskFailed
						slog.Error("map task failed after max attempts (via timeout)",
							"job_id", j.id, "task_id", task.id, "file", task.filePath)
					} else {
						task.state = taskIdle
						tasksReclaimed.WithLabelValues("map").Inc()
					}
				}
			}
			for i := range j.reduceTasks {
				task := &j.reduceTasks[i]
				if task.workerAddr == addr && task.state != taskIdle && task.state != taskFailed {
					if task.state == taskDone {
						j.reduceDone--
					}
					if task.attempts >= maxTaskAttempts {
						task.state = taskFailed
						slog.Error("reduce task failed after max attempts (via timeout)",
							"job_id", j.id, "task_id", task.id)
					} else {
						task.state = taskIdle
						tasksReclaimed.WithLabelValues("reduce").Inc()
					}
				}
			}
			if hasFailedTasks(j) && j.status == jobRunning {
				j.status = jobFailed
				slog.Warn("job marked as failed", "job_id", j.id)
			}
		}

		delete(m.lastSeen, addr)
		for i, w := range m.workers {
			if w == addr {
				m.workers = append(m.workers[:i], m.workers[i+1:]...)
				break
			}
		}
		workersRegistered.Set(float64(len(m.workers)))
	}
}

func (m *masterImpl) SubmitJob(_ context.Context,
	req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	jobID := m.nextJobID
	m.nextJobID++

	numPartitions := int(NUM_WORKERS)

	mapTasks := make([]mapTask, len(req.FilePaths))
	for i, path := range req.FilePaths {
		mapTasks[i] = mapTask{id: i, filePath: path, state: taskIdle}
	}

	reduceTasks := make([]reduceTask, numPartitions)
	for i := range numPartitions {
		reduceTasks[i] = reduceTask{id: len(mapTasks) + i, partition: i, state: taskIdle}
	}

	m.jobs[jobID] = &job{
		id:            jobID,
		files:         req.FilePaths,
		numPartitions: numPartitions,
		mapTasks:      mapTasks,
		reduceTasks:   reduceTasks,
		status:        jobRunning,
	}

	slog.Info("job submitted", "job_id", jobID, "num_files", len(req.FilePaths))
	return &pb.SubmitJobResponse{JobId: jobID}, nil
}

func (m *masterImpl) GetJobStatus(_ context.Context,
	req *pb.GetJobStatusRequest) (*pb.GetJobStatusResponse, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	j, ok := m.jobs[req.JobId]
	if !ok {
		return &pb.GetJobStatusResponse{Status: pb.GetJobStatusResponse_UNKNOWN}, nil
	}

	resp := &pb.GetJobStatusResponse{}
	switch j.status {
	case jobDone:
		resp.Status = pb.GetJobStatusResponse_DONE
		resp.IndexPath = j.indexPath
	case jobFailed:
		resp.Status = pb.GetJobStatusResponse_FAILED
		resp.ErrorMessage = "one or more tasks exceeded max attempts"
	default:
		resp.Status = pb.GetJobStatusResponse_RUNNING
	}
	return resp, nil
}

func sortedJobIDs(m map[int32]*job) []int32 {
	ids := make([]int32, 0, len(m))
	for k := range m {
		ids = append(ids, k)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (m *masterImpl) finalizeJob(j *job) {
	log := slog.With("job_id", j.id, "phase", "finalize")
	log.Info("finalizing job")

	m.mtx.Lock()
	addrs := make([]string, len(j.reduceTasks))
	for i, t := range j.reduceTasks {
		addrs[i] = t.workerAddr
	}
	dir := m.dir
	jobID := j.id
	m.mtx.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	indexPath := filepath.Join(dir, "index-"+strconv.FormatInt(int64(jobID), 10))
	f, e := os.Create(indexPath)
	check(e)
	defer f.Close()
	w := bufio.NewWriter(f)

	for partition, addr := range addrs {
		if addr == "" {
			continue
		}
		fetchResultFromWorkerForJob(ctx, addr, partition, jobID, w)
	}
	check(w.Flush())

	m.mtx.Lock()
	j.status = jobDone
	j.indexPath = indexPath
	m.mtx.Unlock()

	log.Info("job finalized", "index_path", indexPath)
}

func hasFailedTasks(j *job) bool {
	for _, t := range j.mapTasks {
		if t.state == taskFailed {
			return true
		}
	}
	for _, t := range j.reduceTasks {
		if t.state == taskFailed {
			return true
		}
	}
	return false
}
