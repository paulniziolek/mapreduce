package mapreduce

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"

	"github.com/paulniziolek/mapreduce/pkg/mapreduce/task"
)

type KeyValue struct {
	Key   string
	Value string
}

var ( 
	intermediateFileFormat = "mr-%d-%d"
	finalFileFormat = "mr-out-%d"
)

// ihash(key) % nReduce to split keys across nReduce reduce tasks
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		resp := &GetTaskResponse{}
		call("Master.GetTask", &GetTaskRequest{}, resp)
		t := resp.Task

		if t.TaskType == task.Map {
			processMapTask(mapf, t)
		} else if t.TaskType == task.Reduce {
			processReduceTask(reducef, t)
		} else if t.TaskType == task.Exit {
			fmt.Println("Exit task received, exiting...")
			os.Exit(0)
		}

	}
}

func processMapTask(mapf func(string, string) []KeyValue, t *task.Task) {
	file, err := os.Open(t.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", t.FileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", t.FileName)
	}
	file.Close()

	kva := mapf(t.FileName, string(content))

	// TODO: process KV pairs and emit to intermediate files
}

func processReduceTask(reducef func(string, []string) string, t *task.Task) {
	// TODO: IMPLEMENT

}

// send an RPC request to the master, wait for the response.
// usually returns true. returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
