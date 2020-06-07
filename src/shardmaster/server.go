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

	sm.adjustConfig(&config)
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
	sm.adjustConfig(&config)
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

func (sm *ShardMaster) adjustConfig(config *Config) {
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}
	} else if len(config.Groups) == 1 {
		for g := range config.Groups {
			for s := range config.Shards {
				config.Shards[s] = g
			}
		}
	} else if len(config.Groups) <= NShards {
		avg := NShards / len(config.Groups)
		shardsRemain := NShards - avg*len(config.Groups)
		needLoop := false
		lastGid := 0
	LOOP:
		var groups []int
		for g := range config.Groups {
			groups = append(groups, g)
		}
		sort.Ints(groups)
		for _, gid := range groups {
			lastGid = gid
			count := 0
			for _, shardGid := range config.Shards {
				if shardGid == gid {
					count++
				}
			}
			if count == avg {
				continue
			} else if count > avg && shardsRemain == 0 {
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg {
							config.Shards[i] = 0
						} else {
							c++
						}
					}
				}
			} else if count > avg && shardsRemain > 0 {
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg+shardsRemain {
							config.Shards[i] = 0
						} else {
							if c == avg {
								shardsRemain -= 1
							} else {
								c += 1
							}
						}
					}
				}
			} else {
				for i, val := range config.Shards {
					if count == avg {
						break
					}
					if val == 0 && count < avg {
						config.Shards[i] = gid
					}
				}
				if count < avg {
					needLoop = true
				}
			}
		}
		if needLoop {
			needLoop = false
			goto LOOP
		}
		if lastGid != 0 {
			for i, val := range config.Shards {
				if val == 0 {
					config.Shards[i] = lastGid
				}
			}
		}
	} else {
		gids := make(map[int]int)
		emptyShards := make([]int, 0, NShards)
		for i, gid := range config.Shards {
			if gid == 0 {
				emptyShards = append(emptyShards, i)
				continue
			}
			if _, ok := gids[gid]; ok {
				emptyShards = append(emptyShards, i)
				config.Shards[i] = 0
			} else {
				gids[gid] = 1
			}
		}
		n := 0
		if len(emptyShards) > 0 {
			var groups []int
			for k := range config.Groups {
				groups = append(groups, k)
			}
			sort.Ints(groups)
			for _, gid := range groups {
				if _, ok := gids[gid]; !ok {
					config.Shards[emptyShards[n]] = gid
					n += 1
				}
				if n >= len(emptyShards) {
					break
				}
			}
		}
	}
}
