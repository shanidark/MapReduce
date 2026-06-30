package main

import (
	"context"
	"log"
	pb "mapreduce/proto"
	"time"
)

func (m *masterImpl) RegisterWorker(_ context.Context,
	info *pb.WorkerInfo) (*pb.Ack, error) {
	log.Printf("RegisterWorker called by %s, waiting for lock...", info.Addr)
	m.mtx.Lock()
	log.Printf("RegisterWorker got lock for %s", info.Addr)
	defer m.mtx.Unlock()

	for _, addr := range m.workers {
		if addr == info.Addr {
			m.lastSeen[info.Addr] = time.Now()
			log.Printf("worker %s registered again (already known)", info.Addr)
			return &pb.Ack{}, nil
		}
	}

	m.workers = append(m.workers, info.Addr)
	m.lastSeen[info.Addr] = time.Now()
	log.Printf("registered worker: %s, total: %d", info.Addr, len(m.workers))
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
			log.Printf("→ task to %s: type=%v id=%d", req.WorkerAddr, pb.Task_MAP, int32(task.id))
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
			log.Printf("→ task to %s: type=%v id=%d", req.WorkerAddr, pb.Task_REDUCE, int32(task.id))
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
			log.Printf("worker %s timed out, reclaiming tasks", addr)
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
		}
	}
}
