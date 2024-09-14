package task

type TaskStatus string

const (
	Idle          TaskStatus = "IDLE"
	Processing    TaskStatus = "PROCESSING"
	Done          TaskStatus = "DONE"
	UnknownStatus TaskStatus = ""
)
