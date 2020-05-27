package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	taskMaxInterval = 5 * time.Second
)

var (
	errNoTask = rpc.ServerError("no task")
)

type Master struct {
	sync.RWMutex

	files   []string
	mTasks  []*Task
	rTasks  []*Task
	nMap    int
	nReduce int
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.Lock()
	defer m.Unlock()

	if len(m.mTasks) == 0 && len(m.rTasks) == 0 {
		return errNoTask
	}

	for _, task := range m.mTasks {
		if task.WorkerID == "" {
			task.WorkerID = args.WorkerID
			task.StartTime = time.Now()
			reply.Task = task
			return nil
		}
	}

	if len(m.mTasks) > 0 {
		return rpc.ServerError("map not done")
	}

	for _, task := range m.rTasks {
		if task.WorkerID == "" {
			task.WorkerID = args.WorkerID
			task.StartTime = time.Now()
			reply.Task = task
			return nil
		}
	}
	return rpc.ServerError("no task available")
}

func (m *Master) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	m.Lock()
	defer m.Unlock()

	switch args.Mode {
	case TaskModeMap:
		newTasks := make([]*Task, 0)
		for _, task := range m.mTasks {
			if task.ID == args.TaskID {
				if task.WorkerID != args.WorkerID {
					return rpc.ServerError("invalid worker")
				}
				continue
			}
			newTasks = append(newTasks, task)
		}
		m.mTasks = newTasks
	case TaskModeReduce:
		newTasks := make([]*Task, 0)
		for _, task := range m.rTasks {
			if task.ID == args.TaskID {
				if task.WorkerID != args.WorkerID {
					return rpc.ServerError("invalid worker")
				}
				continue
			}
			newTasks = append(newTasks, task)
		}
		m.rTasks = newTasks
	default:
		return rpc.ServerError("invalid task mode")
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.RLock()
	defer m.RUnlock()
	return len(m.mTasks) == 0 && len(m.rTasks) == 0
}

func (m *Master) cleanTaskLoop() {
	for ; ; time.Sleep(500 * time.Millisecond) {
		m.Lock()
		if len(m.mTasks) == 0 && len(m.rTasks) == 0 {
			m.Unlock()
			return
		}
		for _, task := range m.mTasks {
			if time.Since(task.StartTime) > taskMaxInterval {
				task.WorkerID = ""
			}
		}
		for _, task := range m.rTasks {
			if time.Since(task.StartTime) > taskMaxInterval {
				task.WorkerID = ""
			}
		}
		m.Unlock()
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	mTasks, rTasks := make([]*Task, 0), make([]*Task, 0)
	for i, f := range files {
		mTasks = append(mTasks, &Task{
			ID:       i,
			Mode:     TaskModeMap,
			FileName: f,
			NMap:     len(files),
			NReduce:  nReduce,
		})
	}
	for i := 0; i < nReduce; i++ {
		rTasks = append(rTasks, &Task{
			ID:       i,
			Mode:     TaskModeReduce,
			FileName: "",
			NMap:     len(files),
			NReduce:  nReduce,
		})
	}
	m := &Master{
		RWMutex: sync.RWMutex{},
		files:   files,
		mTasks:  mTasks,
		rTasks:  rTasks,
		nReduce: nReduce,
	}

	go m.cleanTaskLoop()

	m.server()
	return m
}
