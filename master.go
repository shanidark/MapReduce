package main

import (
	"context"
	"log/slog"
	pb "mapreduce/proto"
	"time"
)

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
	slog.Info("registered worker", "worker_addr", info.Addr, "total_workers", len(m.workers))
	return &pb.Ack{}, nil
}

func (m *masterImpl) RequestTask(_ context.Context,
	req *pb.TaskRequest) (*pb.Task, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if len(m.workers) < m.minWorkers {
		return &pb.Task{Type: pb.Task_IDLE}, nil
	}

	// разадача мап задач
	for i := range m.mapTasks {
		task := &m.mapTasks[i]
		if task.state == taskIdle {
			task.state = taskRunning
			task.workerAddr = req.WorkerAddr
			slog.Info("given task", "worker_addr", req.WorkerAddr, "task_type", pb.Task_MAP, "task_id", int32(task.id))
			return &pb.Task{
				Type:          pb.Task_MAP,
				TaskId:        int32(task.id),
				FilePath:      task.filePath,
				NumPartitions: int32(m.numPartitions),
			}, nil
		}
	}

	if m.mapDone < len(m.mapTasks) {
		return &pb.Task{Type: pb.Task_IDLE}, nil
	}

	// раздаем редюс задачи
	for i := range m.reduceTasks {
		task := &m.reduceTasks[i]
		if task.state == taskIdle {
			task.state = taskRunning
			slog.Info("given task", "worker_addr", req.WorkerAddr, "task_type", pb.Task_REDUCE, "task_id", int32(task.id))
			return &pb.Task{
				Type:       pb.Task_REDUCE,
				TaskId:     int32(task.id),
				Partition:  int32(task.partition),
				SpillAddrs: m.workers,
			}, nil
		}
	}
	if !m.allDone {
		return &pb.Task{Type: pb.Task_IDLE}, nil
	}
	return &pb.Task{Type: pb.Task_SHUTDOWN}, nil
}

func (m *masterImpl) ReportDone(_ context.Context,
	done *pb.TaskDone) (*pb.Ack, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for i := range m.mapTasks {
		task := &m.mapTasks[i]
		if task.id == int(done.TaskId) && task.state == taskRunning {
			task.state = taskDone
			m.mapDone++
			return &pb.Ack{}, nil
		}
	}
	for i := range m.reduceTasks {
		task := &m.reduceTasks[i]
		if task.id == int(done.TaskId) && task.state == taskRunning {
			task.state = taskDone
			task.workerAddr = done.WorkerAddr
			m.reduceDone++
			if m.reduceDone == len(m.reduceTasks) {
				close(m.done)
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
		if now.Sub(last) > timeout {
			slog.Warn("worker time out", "worker_addr", addr, "last_seen", last)
			for i := range m.mapTasks {
				if m.mapTasks[i].state == taskRunning && m.mapTasks[i].workerAddr == addr {
					m.mapTasks[i].state = taskIdle
				}
			}
			for i := range m.reduceTasks {
				if m.reduceTasks[i].state == taskRunning && m.reduceTasks[i].workerAddr == addr {
					m.reduceTasks[i].state = taskIdle
				}
			}
			delete(m.lastSeen, addr)
			for i, w := range m.workers {
				if w == addr {
					m.workers = append(m.workers[:i], m.workers[i+1:]...)
					break
				}
			}
		}
	}
}
