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
	// mapping input files to their MapTask definition
	mapTasks map[string]*task.MapTask
	// mapping reduce job IDs to their ReduceTask definition
	reduceTasks map[int]*task.ReduceTask

	reducerCount int

	// asynchronously update tasks
	tasklock sync.Mutex
}

func (m *Master) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	// look for map tasks
	m.tasklock.Lock()
	defer m.tasklock.Unlock()

	mapTask := m.getMapTask()
	if mapTask != nil {
		updateMapTask(mapTask)
		reply.MapTask = mapTask
		return nil
	}

	// Check if all map tasks are done / if its OK to assign reduce tasks
	// otherwise, design behavior around GetTask with nondeterminate tasks
	// TODO: what to do when no tasks available but MapReduce is still not Done

	reduceTask := m.getReduceTask()
	if reduceTask != nil {
		// don't know yet if I need to update reduce task, as I don't know if reduce workers can fail / how to handle if they're slow.
		reply.ReduceTask = reduceTask
		return nil
	}

	return nil
}

func (m *Master) getMapTask() *task.MapTask {
	for _, t := range m.mapTasks {
		if isProcessableMapTask(t) {
			return t
		}
	}
	return nil
}

func (m *Master) getReduceTask() *task.ReduceTask {
	for _, t := range m.reduceTasks {
		if t.Status == task.Idle {
			return t
		}
	}
	return nil
}

func isProcessableMapTask(t *task.MapTask) bool {
	return t.Status == task.Idle || (t.Status == task.Processing && time.Now().Unix()-t.LastProcessed >= TaskTimeout)
}

func updateMapTask(t *task.MapTask) {
	t.Status = task.Processing
	t.LastProcessed = time.Now().Unix()
}

func (m *Master) ReportMap(args *ReportMapRequest, reply *ReportMapReply) {
	m.tasklock.Lock()
	defer m.tasklock.Unlock()

	mapTask := m.mapTasks[args.InputFile]
	mapTask.Status = task.Done

	// TODO: add generated intermediate files to respective Reduce Tasks
}

func (m *Master) ReportReduce(args *ReportReduceRequest, reply *ReportReduceReply) {
	m.tasklock.Lock()
	defer m.tasklock.Unlock()

	reduceTask := m.reduceTasks[args.ReducerID]
	reduceTask.Status = task.Done
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.tasklock.Lock()
	defer m.tasklock.Unlock()

	done := true
	for _, t := range m.reduceTasks {
		if t.Status != task.Done {
			done = false
		}
	}

	return done
}

func MakeMaster(files []string, nReduce int) *Master {
	mapTasksMap := make(map[string]*task.MapTask)
	currMapID := 0 // incremental ID

	for _, file := range files {
		mapTasksMap[file] = &task.MapTask{
			ID:           currMapID,
			Status:       task.Idle,
			InputFile:    file,
			ReducerCount: nReduce,
		}
		currMapID++
	}

	reduceTasksMap := make(map[int]*task.ReduceTask)
	for i := 0; i < nReduce; i++ {
		reduceTasksMap[i] = &task.ReduceTask{
			ID:     i,
			Status: task.Idle,
		}
	}

	m := Master{
		mapTasks:     mapTasksMap,
		reduceTasks:  reduceTasksMap,
		reducerCount: nReduce,
	}
	m.server()
	return &m
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
