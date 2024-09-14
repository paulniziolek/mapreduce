package mapreduce

import (
	"encoding/json"
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
	finalFileFormat        = "mr-out-%d"
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
		success := call("Master.GetTask", &GetTaskRequest{}, resp)
		if !success {
			// Assume master has finished
			os.Exit(0)
		}
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
	filename := t.MapMetadata.InputFile
	reduceWorkers := t.MapMetadata.ReduceWorkers

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))

	kvBuckets := createKVBuckets(kva, reduceWorkers)

	intermediateFiles, _ := writeIntermediateFiles(t.ID, kvBuckets)

	call("Master.FinishTask",
		&FinishTaskRequest{
			Task:       t,
			FileOutput: intermediateFiles,
		},
		&FinishTaskResponse{},
	)
}

func createKVBuckets(kva []KeyValue, reduceWorkers int) [][]*KeyValue {
	buckets := make([][]*KeyValue, reduceWorkers)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % reduceWorkers
		buckets[bucket] = append(buckets[bucket], &kv)
	}
	return buckets
}

func writeIntermediateFiles(mapID int, buckets [][]*KeyValue) ([]string, error) {
	createdFiles := make([]string, len(buckets))

	for i, bucket := range buckets {
		createdFiles[i] = fmt.Sprintf(intermediateFileFormat, mapID, i)
		file, _ := os.Create(createdFiles[i])
		enc := json.NewEncoder(file)
		defer file.Close()

		for _, kv := range bucket {
			enc.Encode(kv)
		}
	}

	return createdFiles, nil
}

func processReduceTask(reducef func(string, []string) string, t *task.Task) {
	// TODO: IMPLEMENT
	log.Fatalf("received reduce task, unimplemented...")
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
