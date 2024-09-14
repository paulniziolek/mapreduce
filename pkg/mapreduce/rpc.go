package mapreduce

import (
	"os"
	"strconv"

	"github.com/paulniziolek/mapreduce/pkg/mapreduce/task"
)

type GetTaskRequest struct{}

type GetTaskResponse struct {
	Task *task.Task
}

type FinishTaskRequest struct {
	Task       *task.Task
	FileOutput []string
}

type FinishTaskResponse struct{}

func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
