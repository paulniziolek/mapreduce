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

	mapTask := m.getMapTask()
	if mapTask != nil {
		updateMapTask(mapTask)
		reply.MapTask = mapTask
		m.tasklock.Unlock()
		log.Printf("Assigned map task %d to worker", mapTask.ID)
		return nil
	}

	if !m.MapTasksDone() {
		m.tasklock.Unlock()
		log.Println("Telling worker to wait for other Map Tasks")
		return nil
	}

	reduceTask := m.getReduceTask()
	if reduceTask != nil {
		updateReduceTask(reduceTask)
		reply.ReduceTask = reduceTask
		m.tasklock.Unlock()
		log.Printf("Assigned reduce task %d to worker", reduceTask.ID)
		return nil
	}

	m.tasklock.Unlock()
	log.Println("Returned DONE status to worker")
	reply.Done = m.Done()
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
		if isProcessableReduceTask(t) {
			return t
		}
	}
	return nil
}

func isProcessableMapTask(t *task.MapTask) bool {
	return t.Status == task.Idle || (t.Status == task.Processing && time.Now().Unix()-t.LastProcessed >= TaskTimeout)
}

func isProcessableReduceTask(t *task.ReduceTask) bool {
	return t.Status == task.Idle || (t.Status == task.Processing && time.Now().Unix()-t.LastProcessed >= TaskTimeout)
}

func updateMapTask(t *task.MapTask) {
	t.Status = task.Processing
	t.LastProcessed = time.Now().Unix()
}

func updateReduceTask(t *task.ReduceTask) {
	t.Status = task.Processing
	t.LastProcessed = time.Now().Unix()
}

func (m *Master) ReportMap(args *ReportMapRequest, reply *ReportMapReply) error {
	m.tasklock.Lock()
	defer m.tasklock.Unlock()

	mapTask := m.mapTasks[args.InputFile]
	mapTask.Status = task.Done

	for i, file := range args.IntermediateFiles {
		m.reduceTasks[i].IntermediateFiles = append(m.reduceTasks[i].IntermediateFiles, file)
	}
	log.Println("Received worker call to ReportMap")
	return nil
}

func (m *Master) ReportReduce(args *ReportReduceRequest, reply *ReportReduceReply) error {
	m.tasklock.Lock()
	defer m.tasklock.Unlock()

	reduceTask := m.reduceTasks[args.ReducerID]
	reduceTask.Status = task.Done
	log.Println("Received worker call to ReportReduce")
	return nil
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

// Caller requires lock
func (m *Master) MapTasksDone() bool {
	done := true
	for _, t := range m.mapTasks {
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
	// TODO: Handle Master Ticker logic to check for crashed/slow workers
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
