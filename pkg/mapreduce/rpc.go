package mapreduce

import (
	"os"
	"strconv"

	"github.com/paulniziolek/mapreduce/pkg/mapreduce/task"
)

type GetTaskRequest struct{}

type GetTaskResponse struct {
	MapTask    *task.MapTask
	ReduceTask *task.ReduceTask
	Done       bool
}

type ReportMapRequest struct {
	InputFile         string
	IntermediateFiles []string
}

type ReportMapReply struct{}

type ReportReduceRequest struct {
	ReducerID int
}

type ReportReduceReply struct{}

func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
