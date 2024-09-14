package task

type Task struct {
	ID          int64 // ID corresponds with either Map Task ID, or Reduce Task ID (where in the case of Reduce, ID < nReduce)
	FileName    string
	TaskType    TaskType
	Status      TaskStatus
	LastUpdated int64
}
