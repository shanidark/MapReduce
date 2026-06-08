package main

import (
	"context"
	"log"
	pb "mapreduce/proto"
	"sync"
)

type taskState int

const (
	taskIdle taskState = iota
	taskRunning
	taskDone
)

type masterImpl struct {
	pb.UnimplementedMasterServer

	mtx        sync.Mutex
	workers    []string
	minWorkers int

	mapTasks      []mapTask
	reduceTasks   []reduceTask
	mapDone       int
	reduceDone    int
	numPartitions int

	allDone bool
	done    chan struct{}
}

type mapTask struct {
	id       int
	filePath string
	state    taskState
}

type reduceTask struct {
	id         int
	partition  int
	state      taskState
	workerAddr string
}

func (m *masterImpl) RegisterWorker(_ context.Context,
	info *pb.WorkerInfo) (*pb.Ack, error) {
	log.Printf("RegisterWorker called by %s, waiting for lock...", info.Addr)
	m.mtx.Lock()
	log.Printf("RegisterWorker got lock for %s", info.Addr)
	defer m.mtx.Unlock()
	m.workers = append(m.workers, info.Addr)
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
		log.Printf("→ task to %s: type=%v", req.WorkerAddr, pb.Task_IDLE)
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
