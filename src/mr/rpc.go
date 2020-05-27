package mr

import (
	"os"
	"strconv"
)

type GetTaskArgs struct {
	WorkerID string
}

type GetTaskReply struct {
	Task *Task
}

type FinishTaskArgs struct {
	TaskID   int
	Mode     TaskMode
	WorkerID string
}

type FinishTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
