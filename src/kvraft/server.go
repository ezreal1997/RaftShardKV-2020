package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/multierr"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const WaitOPTimeOut = time.Millisecond * 500

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	MsgID    msgID
	ReqID    int64
	ClientID int64
	Key      string
	Value    string
	Method   string
}

type NotifyMsg struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stopCh      chan struct{}
	msgNotify   map[int64]chan NotifyMsg
	lastApplies map[int64]msgID // last apply put/append msg
	data        map[string]string
	persister   *raft.Persister
}

func (kv *KVServer) waitOP(op Op) (res NotifyMsg) {
	if _, _, isLeader := kv.rf.Start(op); !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan NotifyMsg, 1)
	kv.msgNotify[op.ReqID] = ch
	kv.mu.Unlock()

	t := time.NewTimer(WaitOPTimeOut)
	defer t.Stop()
	select {
	case res = <-ch:
		kv.mu.Lock()
		delete(kv.msgNotify, op.ReqID)
		kv.mu.Unlock()
		return
	case <-t.C:
		kv.mu.Lock()
		delete(kv.msgNotify, op.ReqID)
		kv.mu.Unlock()
		res.Err = ErrTimeOut
		return
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		MsgID:    args.MsgID,
		ReqID:    nrand(),
		Key:      args.Key,
		Method:   "Get",
		ClientID: args.ClientID,
	}
	res := kv.waitOP(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		MsgID:    args.MsgID,
		ReqID:    nrand(),
		Key:      args.Key,
		Value:    args.Value,
		Method:   args.Op,
		ClientID: args.ClientID,
	}
	reply.Err = kv.waitOP(op).Err
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.stopCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := &KVServer{
		mu:           sync.Mutex{},
		me:           me,
		applyCh:      make(chan raft.ApplyMsg),
		dead:         0,
		maxraftstate: maxraftstate,
		stopCh:       make(chan struct{}),
		msgNotify:    make(map[int64]chan NotifyMsg),
		lastApplies:  make(map[int64]msgID),
		data:         make(map[string]string),
		persister:    persister,
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readPersist(kv.persister.ReadSnapshot())

	go kv.applyLoop()

	return kv
}

func (kv *KVServer) applyLoop() {
	for {
		select {
		case <-kv.stopCh:
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				kv.mu.Lock()
				kv.readPersist(kv.persister.ReadSnapshot())
				kv.mu.Unlock()
				continue
			}

			msgIdx := msg.CommandIndex
			op := msg.Command.(Op)
			var isRepeated bool
			kv.mu.Lock()
			if val, ok := kv.lastApplies[op.ClientID]; ok {
				isRepeated = val == op.MsgID
			} else {
				isRepeated = false
			}
			switch op.Method {
			case "Put":
				if !isRepeated {
					kv.data[op.Key] = op.Value
					kv.lastApplies[op.ClientID] = op.MsgID
				}
			case "Append":
				if !isRepeated {
					var val string
					if v, ok := kv.data[op.Key]; ok {
						val = v
					} else {
						val = ""
					}
					kv.data[op.Key] = val + op.Value
					kv.lastApplies[op.ClientID] = op.MsgID
				}
			case "Get":
			default:
				log.Fatalf("unknown method: %s", op.Method)
			}
			kv.saveSnapshot(msgIdx)
			if ch, ok := kv.msgNotify[op.ReqID]; ok {
				var val string
				if v, ok := kv.data[op.Key]; ok {
					val = v
				} else {
					val = ""
				}
				ch <- NotifyMsg{
					Err:   OK,
					Value: val,
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}

	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	if err := enc.Encode(kv.data); err != nil {
		panic(err)
	}
	if err := enc.Encode(kv.lastApplies); err != nil {
		panic(err)
	}
	kv.rf.SavePersistAndSnapshot(logIndex, buf.Bytes())
}

func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)

	kvData, lastApplies := make(map[string]string), make(map[int64]msgID)
	if err := multierr.Combine(
		dec.Decode(&kvData),
		dec.Decode(&lastApplies),
	); err != nil {
		log.Fatalf("kv server read persist failed: %v", err)
	} else {
		kv.data = kvData
		kv.lastApplies = lastApplies
	}
}
