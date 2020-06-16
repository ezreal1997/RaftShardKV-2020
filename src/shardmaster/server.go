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
	} else if len(config.Groups) <= NShards {
		avg := NShards / len(config.Groups)
		redundantShards := NShards - avg*len(config.Groups)
		var lastGid, shardCount int
		for shardCount < avg {
			var groups []int
			for g := range config.Groups {
				groups = append(groups, g)
			}
			sort.Ints(groups)
			for _, gid := range groups {
				lastGid, shardCount = gid, 0
				for _, shardGid := range config.Shards {
					if shardGid == gid {
						shardCount++
					}
				}
				if shardCount == avg {
					continue
				} else if shardCount > avg && redundantShards == 0 {
					num := 0
					for shardNum, shardGid := range config.Shards {
						if shardGid == gid {
							if num == avg {
								config.Shards[shardNum] = 0
							} else {
								num++
							}
						}
					}
				} else if shardCount > avg && redundantShards > 0 {
					num := 0
					for shardNum, shardGid := range config.Shards {
						if shardGid == gid {
							if num == avg+redundantShards {
								config.Shards[shardNum] = 0
							} else {
								if num == avg {
									redundantShards--
								} else {
									num++
								}
							}
						}
					}
				} else {
					for shardNum, shardGid := range config.Shards {
						if shardCount == avg {
							break
						}
						if shardGid == 0 && shardCount < avg {
							config.Shards[shardNum] = gid
						}
					}
				}
			}
		}
		if lastGid != 0 {
			for shardNum, shardGid := range config.Shards {
				if shardGid == 0 {
					config.Shards[shardNum] = lastGid
				}
			}
		}
	} else {
		gidSet := make(map[int]struct{})
		emptyShards := make([]int, 0, NShards)
		for shardNum, shardGid := range config.Shards {
			if shardGid == 0 {
				emptyShards = append(emptyShards, shardNum)
				continue
			}
			if _, ok := gidSet[shardGid]; ok {
				emptyShards = append(emptyShards, shardNum)
				config.Shards[shardNum] = 0
			} else {
				gidSet[shardGid] = struct{}{}
			}
		}
		pos := 0
		if len(emptyShards) > 0 {
			var groups []int
			for gid := range config.Groups {
				groups = append(groups, gid)
			}
			sort.Ints(groups)
			for _, gid := range groups {
				if _, ok := gidSet[gid]; !ok {
					config.Shards[emptyShards[pos]] = gid
					pos++
				}
				if pos >= len(emptyShards) {
					break
				}
			}
		}
	}
}
