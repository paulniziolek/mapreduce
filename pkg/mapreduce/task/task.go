package task

type MapTask struct {
	ID            int
	Status        TaskStatus
	LastProcessed int64
	InputFile     string
	ReducerCount  int
}

type ReduceTask struct {
	ID                int
	Status            TaskStatus
	IntermediateFiles []string
}
