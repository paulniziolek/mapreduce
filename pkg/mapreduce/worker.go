package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/paulniziolek/mapreduce/pkg/mapreduce/task"
)

type KeyValue struct {
	Key   string
	Value string
}

type KeyValueList []KeyValue

func (a KeyValueList) Len() int           { return len(a) }
func (a KeyValueList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KeyValueList) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	GetTask      string = "Master.GetTask"
	ReportMap    string = "Master.ReportMap"
	ReportReduce string = "Master.ReportReduce"
)

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
		call(GetTask, &GetTaskRequest{}, resp)
		if resp.Done {
			log.Println("Recieved DONE, exiting...")
			os.Exit(0)
		}

		if resp.MapTask != nil {
			log.Printf("Doing Map Task %d ON %s", resp.MapTask.ID, resp.MapTask.InputFile)
			processMapTask(mapf, resp.MapTask)
		} else if resp.ReduceTask != nil {
			log.Printf("Doing Reduce Task %d", resp.ReduceTask.ID)
			processReduceTask(reducef, resp.ReduceTask)
		} else {
			// Polling
			log.Println("Awaiting next task")
			time.Sleep(1 * time.Second)
		}
	}
}

func processMapTask(mapf func(string, string) []KeyValue, task *task.MapTask) {
	filename := task.InputFile

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

	kvBuckets := createKVBuckets(kva, task.ReducerCount)

	intermediateFiles, _ := writeIntermediateFiles(task.ID, kvBuckets)

	log.Printf("Reporting Map Task Done on %s, with intermediate files %v", filename, intermediateFiles)
	call(ReportMap,
		&ReportMapRequest{
			InputFile:         filename,
			IntermediateFiles: intermediateFiles,
		},
		&ReportMapReply{},
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

func processReduceTask(reducef func(string, []string) string, t *task.ReduceTask) {
	outputFileName := fmt.Sprintf(finalFileFormat, t.ID)
	outputFile, _ := os.CreateTemp("", outputFileName)
	defer outputFile.Close()

	aggregate := []KeyValue{}

	for _, f := range t.IntermediateFiles {
		file, _ := os.Open(f)

		contents, _ := io.ReadAll(file)
		var kva []KeyValue
		json.Unmarshal(contents, &kva)
		aggregate = append(aggregate, kva...)
		file.Close()
	}

	sort.Sort(KeyValueList(aggregate))

	enc := json.NewEncoder(outputFile)
	i := 0
	for i < len(aggregate) {
		key := aggregate[i].Key
		values := []string{aggregate[i].Value}

		var j int
		for j = 0; j < len(aggregate) && aggregate[j].Key == key; j++ {
			values = append(values, aggregate[j].Value)
		}
		i = j

		reducedValue := reducef(key, values)
		enc.Encode(KeyValue{Key: key, Value: reducedValue})
	}
	os.Rename(outputFile.Name(), outputFileName)

	log.Printf("Reporting Reduce Task %d Done on %v, with output files %s", t.ID, t.IntermediateFiles, outputFile.Name())
	call(ReportReduce,
		&ReportReduceRequest{
			t.ID,
		},
		&ReportReduceReply{},
	)
}

// send an RPC request to the master, wait for the response.
// usually returns true. returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("Error! Failed connect", err.Error())
		time.Sleep(1 * time.Second)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
