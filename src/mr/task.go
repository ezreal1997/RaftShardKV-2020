package mr

import (
	"fmt"
	"time"
)

type TaskMode int

const (
	TaskModeMap = iota
	TaskModeReduce
)

type Task struct {
	ID        int
	Mode      TaskMode
	FileName  string
	NMap      int
	NReduce   int
	WorkerID  string
	StartTime time.Time
}

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
