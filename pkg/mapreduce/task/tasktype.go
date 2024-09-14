package task

type TaskType string

const (
	Map    TaskType = "MAP"
	Reduce TaskType = "REDUCE"

	// pseudo-task type
	Exit TaskType = "EXIT"

	UnknownType TaskType = ""
)
