package mapreduce

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/paulniziolek/mapreduce/pkg/mapreduce/task"
)

const TaskTimeout = 10

type Master struct {
	// Your definitions here.
	tasks []*task.Task

	// asynchronously update tasks
	tasklock sync.Mutex

	// condition once all map tasks have been done
	mapcond *sync.Cond

	nReduce int

	mapDone    bool
	reduceDone bool
}

func (m *Master) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	// look for map tasks
	m.tasklock.Lock()
	defer m.tasklock.Unlock()

	for {
		for _, t := range m.tasks {
			if t.TaskType == task.Map && isProcessableTask(t) {
				reply.Task = t
				updateTask(t)
				return nil
			}
		}

		if m.mapDone {
			for _, t := range m.tasks {
				if t.TaskType == task.Reduce && isProcessableTask(t) {
					reply.Task = t
					updateTask(t)
					return nil
				}
			}
		}

		if m.mapDone && m.reduceDone {
			reply.Task = &task.Task{TaskType: task.Exit}
			return nil
		}

		m.mapcond.Wait()
	}
}

func isProcessableTask(t *task.Task) bool {
	return t.Status == task.Idle || (t.Status == task.Processing && time.Now().Unix()-t.LastUpdated >= TaskTimeout)
}

func updateTask(t *task.Task) {
	t.Status = task.Processing
	t.LastUpdated = time.Now().Unix()
}

func (m *Master) FinishTask(args *FinishTaskRequest, reply *FinishTaskResponse) {
	m.tasklock.Lock()
	defer m.tasklock.Unlock()

	args.Task.Status = task.Done
	args.Task.LastUpdated = time.Now().Unix()

	// TODO: IMPLEMENT

}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := m.mapDone && m.reduceDone

	return ret
}

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{nReduce: nReduce}

	m.server()
	return &m
}
