package task

type Task struct {
	ID          int // ID corresponds with either Map Task ID, or Reduce Task ID (where in the case of Reduce, ID < nReduce)
	TaskType    TaskType
	Status      TaskStatus
	LastUpdated int64

	MapMetadata    *MapMetadata
	ReduceMetadata *ReduceMetadata
}

type MapMetadata struct {
	InputFile     string
	ReduceWorkers int
}

type ReduceMetadata struct {
	IntermediateFiles []string
}
