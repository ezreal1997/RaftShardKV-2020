package shardmaster

import (
	"sort"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs     []Config
	msgNotify   map[int64]chan NotifyMsg
	lastApplies map[int64]msgID

	stopCh chan struct{}
}

type Op struct {
	// Your data here.
	MsgID    msgID
	ReqID    int64
	Args     interface{}
	Method   string
	ClientID int64
}

type NotifyMsg struct {
	Err         Err
	WrongLeader bool
	Config      Config
}

func (sm *ShardMaster) execOp(method string, id msgID, clientID int64, args interface{}) NotifyMsg {
	return sm.waitOp(Op{
		MsgID:    id,
		ReqID:    nrand(),
		Args:     args,
		Method:   method,
		ClientID: clientID,
	})
}

func (sm *ShardMaster) waitOp(op Op) (res NotifyMsg) {
	if _, _, isLeader := sm.rf.Start(op); !isLeader {
		res.Err = ErrWrongLeader
		res.WrongLeader = true
		return
	}

	sm.mu.Lock()
	ch := make(chan NotifyMsg, 1)
	sm.msgNotify[op.ReqID] = ch
	sm.mu.Unlock()

	t := time.NewTimer(WaitOpTimeOut)
	defer t.Stop()
	select {
	case res = <-ch:
		sm.mu.Lock()
		delete(sm.msgNotify, op.ReqID)
		sm.mu.Unlock()
		return
	case <-t.C:
		sm.mu.Lock()
		delete(sm.msgNotify, op.ReqID)
		sm.mu.Unlock()
		res.WrongLeader = true
		res.Err = ErrTimeout
		return
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	res := sm.execOp("Join", args.MsgID, args.ClientID, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	res := sm.execOp("Leave", args.MsgID, args.ClientID, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	res := sm.execOp("Move", args.MsgID, args.ClientID, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.mu.Lock()
	if args.Num > 0 && args.Num < len(sm.configs) {
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = sm.getConfigByIndex(args.Num)
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	res := sm.execOp("Query", args.MsgID, args.ClientID, *args)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
	reply.Config = res.Config
}

func (sm *ShardMaster) getConfigByIndex(idx int) Config {
	if idx < 0 || idx >= len(sm.configs) {
		return sm.configs[len(sm.configs)-1].Duplicate()
	} else {
		return sm.configs[idx].Duplicate()
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.stopCh)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	labgob.Register(Op{})

	sm := &ShardMaster{
		mu:          sync.Mutex{},
		me:          me,
		applyCh:     make(chan raft.ApplyMsg, 200),
		configs:     []Config{{Groups: make(map[int][]string)}},
		msgNotify:   make(map[int64]chan NotifyMsg),
		lastApplies: make(map[int64]msgID),
		stopCh:      make(chan struct{}),
	}
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go sm.run()

	return sm
}

func (sm *ShardMaster) run() {
	for {
		select {
		case <-sm.stopCh:
			return
		case msg := <-sm.applyCh:
			if !msg.CommandValid {
				continue
			}
			op := msg.Command.(Op)
			sm.mu.Lock()
			isRepeated := sm.isRepeated(op.ClientID, op.MsgID)
			if !isRepeated {
				switch op.Method {
				case "Join":
					sm.join(op.Args.(JoinArgs))
				case "Leave":
					sm.leave(op.Args.(LeaveArgs))
				case "Move":
					sm.move(op.Args.(MoveArgs))
				case "Query":
				default:
					panic("invalid method")
				}
			}
			res := NotifyMsg{
				Err:         OK,
				WrongLeader: false,
			}
			if op.Method != "Query" {
				sm.lastApplies[op.ClientID] = op.MsgID
			} else {
				res.Config = sm.getConfigByIndex(op.Args.(QueryArgs).Num)
			}
			if ch, ok := sm.msgNotify[op.ReqID]; ok {
				ch <- res
			}
			sm.mu.Unlock()
		}
	}
}

func (sm *ShardMaster) join(args JoinArgs) {
	config := sm.getConfigByIndex(-1)
	config.Num++

	for k, v := range args.Servers {
		config.Groups[k] = v
	}

	sm.reschedule(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) leave(args LeaveArgs) {
	config := sm.getConfigByIndex(-1)
	config.Num++

	for _, gid := range args.GIDs {
		delete(config.Groups, gid)
		for i, v := range config.Shards {
			if v == gid {
				config.Shards[i] = 0
			}
		}
	}
	sm.reschedule(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) move(args MoveArgs) {
	config := sm.getConfigByIndex(-1)
	config.Num++
	config.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs, config)

}

func (sm *ShardMaster) isRepeated(clientID int64, id msgID) bool {
	if val, ok := sm.lastApplies[clientID]; ok {
		return val == id
	}
	return false
}

func (sm *ShardMaster) reschedule(config *Config) {
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}
	} else if len(config.Groups) == 1 {
		for g := range config.Groups {
			for s := range config.Shards {
				config.Shards[s] = g
			}
		}
	} else {
		avg := NShards / len(config.Groups)
		shardsRedundant := NShards - avg*len(config.Groups)
		type groupShards struct {
			gid    int
			shards map[int]struct{}
		}
		groupShardSet := make(map[int]map[int]struct{})
		for gid := range config.Groups {
			groupShardSet[gid] = make(map[int]struct{})
		}
		for i, gid := range config.Shards {
			if _, ok := groupShardSet[gid]; !ok {
				config.Shards[i] = 0
			}
		}
		for s, gid := range config.Shards {
			if gid == 0 {
				continue
			}
			groupShardSet[gid][s] = struct{}{}
		}
		gs := make([]*groupShards, 0)
		for g, s := range groupShardSet {
			gs = append(gs, &groupShards{
				gid:    g,
				shards: s,
			})
		}
		sort.Slice(gs, func(i, j int) bool {
			return len(gs[i].shards) > len(gs[j].shards)
		})
		for i, g := range gs {
			shardNum := len(g.shards)
			if shardNum > avg {
				var extraNum int
				if shardsRedundant > 0 {
					extraNum = 1
				} else {
					extraNum = 0
				}
				for j, gid := range config.Shards {
					if shardNum == avg+extraNum {
						break
					}
					if gid == g.gid {
						config.Shards[j] = 0
						delete(gs[i].shards, j)
						shardNum--
					}
				}
				if extraNum > 0 {
					shardsRedundant--
				}
			} else {
				var extraNum int
				if shardsRedundant > 0 {
					extraNum = 1
				} else {
					extraNum = 0
				}
				for j, gid := range config.Shards {
					if shardNum == avg+extraNum {
						break
					}
					if gid == 0 {
						config.Shards[j] = g.gid
						gs[i].shards[j] = struct{}{}
						shardNum++
					}
				}
				if extraNum > 0 {
					shardsRedundant--
				}
			}
		}
	}
}
