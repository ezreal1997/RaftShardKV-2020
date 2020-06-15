package shardkv

import "../labgob"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

type Err string

func init() {
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(FetchShardDataArgs{})
	labgob.Register(FetchShardDataReply{})
	labgob.Register(DeleteShardDataArgs{})
	labgob.Register(DeleteShardDataArgs{})
	labgob.Register(ShardData{})
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64
	MsgID     int64
	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID  int64
	MsgID     int64
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}

type FetchShardDataArgs struct {
	ConfigNum int
	ShardNum  int
}

type FetchShardDataReply struct {
	Success    bool
	MsgIndexes map[int64]int64
	Data       map[string]string
}

func (reply *FetchShardDataReply) Duplicate() FetchShardDataReply {
	res := FetchShardDataReply{
		Success:    reply.Success,
		Data:       make(map[string]string),
		MsgIndexes: make(map[int64]int64),
	}
	for k, v := range reply.Data {
		res.Data[k] = v
	}
	for k, v := range reply.MsgIndexes {
		res.MsgIndexes[k] = v
	}
	return res
}

type DeleteShardDataArgs struct {
	ConfigNum int
	ShardNum  int
}

type DeleteShardDataReply struct {
	Success bool
}

type ShardData struct {
	ConfigNum  int
	ShardNum   int
	MsgIndexes map[int64]int64
	Data       map[string]string
}
