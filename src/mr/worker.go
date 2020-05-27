package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"

	uuid "github.com/satori/go.uuid"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type worker struct {
	id         string
	mapFunc    func(string, string) []KeyValue
	reduceFunc func(string, []string) string
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	uid, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}
	w := &worker{
		id:         uid.String(),
		mapFunc:    mapf,
		reduceFunc: reducef,
	}
	w.run()
}

func (w *worker) run() {
	for ; ; time.Sleep(time.Second) {
		var reply GetTaskReply
		if err := call("Master.GetTask", &GetTaskArgs{
			WorkerID: w.id,
		}, &reply); errors.Is(err, errNoTask) {
			return
		} else if err != nil {
			log.Printf("%v get task failed: %v", w.id, err)
			continue
		}
		if err := w.doTask(reply.Task); err != nil {
			log.Printf("%v do task %v failed: %v", w.id, reply.Task, err)
			continue
		}
		finishArgs := &FinishTaskArgs{
			TaskID:   reply.Task.ID,
			Mode:     reply.Task.Mode,
			WorkerID: w.id,
		}
		for err := call(
			"Master.FinishTask", finishArgs, &FinishTaskReply{},
		); err != nil; err = call("Master.FinishTask", finishArgs, &FinishTaskReply{}) {
			log.Printf("%v finish task failed: %v, retrying", w.id, err)
		}
	}
}

func (w *worker) doTask(task *Task) error {
	switch task.Mode {
	case TaskModeMap:
		contents, err := ioutil.ReadFile(task.FileName)
		if err != nil {
			return err
		}
		kvs := w.mapFunc(task.FileName, string(contents))
		reduces := make([][]KeyValue, task.NReduce)
		for _, kv := range kvs {
			idx := ihash(kv.Key) % task.NReduce
			reduces[idx] = append(reduces[idx], kv)
		}
		for idx, l := range reduces {
			fileName := reduceName(task.ID, idx)
			f, err := os.Create(fileName)
			if err != nil {
				return err
			}
			enc := json.NewEncoder(f)
			for _, kv := range l {
				if err := enc.Encode(&kv); err != nil {
					f.Close()
					return err
				}
			}
			if err := f.Close(); err != nil {
				return err
			}
		}
	case TaskModeReduce:
		maps := make(map[string][]string)
		for idx := 0; idx < task.NMap; idx++ {
			fileName := reduceName(idx, task.ID)
			f, err := os.Open(fileName)
			if err != nil {
				return err
			}
			dec := json.NewDecoder(f)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err == io.EOF {
					break
				} else if err != nil {
					f.Close()
					return err
				}
				if _, ok := maps[kv.Key]; !ok {
					maps[kv.Key] = make([]string, 0)
				}
				maps[kv.Key] = append(maps[kv.Key], kv.Value)
			}
			f.Close()
		}

		res := make([]string, 0)
		for k, v := range maps {
			res = append(res, fmt.Sprintf("%v %v\n", k, w.reduceFunc(k, v)))
		}

		if err := ioutil.WriteFile(mergeName(task.ID), []byte(strings.Join(res, "")), os.ModePerm); err != nil {
			return err
		}
	default:
		return errors.New("invalid task mode")
	}
	return nil
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err = c.Call(rpcname, args, reply); err != nil {
		return err
	}

	return nil
}
