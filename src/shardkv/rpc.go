package shardkv

import (
	"time"
)

type NotifyMsg struct {
	Err   Err
	Value string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		MsgID:     args.MsgID,
		ReqID:     nrand(),
		Key:       args.Key,
		Operation: "Get",
		ClientID:  args.ClientID,
		ConfigNum: args.ConfigNum,
	}
	res := kv.waitOP(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		MsgID:     args.MsgID,
		ReqID:     nrand(),
		Key:       args.Key,
		Value:     args.Value,
		Operation: args.Op,
		ClientID:  args.ClientID,
		ConfigNum: args.ConfigNum,
	}
	reply.Err = kv.waitOP(op).Err
}

func (kv *ShardKV) FetchShardData(args *FetchShardDataArgs, reply *FetchShardDataReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum >= kv.config.Num {
		return
	}

	if configData, ok := kv.historyShards[args.ConfigNum]; ok {
		if shardData, ok := configData[args.ShardNum]; ok {
			reply.Success = true
			reply.Data = make(map[string]string)
			reply.MsgIndexes = make(map[int64]int64)
			for k, v := range shardData.Data {
				reply.Data[k] = v
			}
			for k, v := range shardData.MsgIndexes {
				reply.MsgIndexes[k] = v
			}
		}
	}
	return
}

func (kv *ShardKV) DeleteShardData(args *DeleteShardDataArgs, reply *DeleteShardDataReply) {
	kv.mu.Lock()

	if args.ConfigNum >= kv.config.Num {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	_, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		return
	}

	ticker := time.NewTicker(time.Millisecond * 20)
	defer ticker.Stop()
	timer := time.NewTimer(time.Millisecond * 500)
	defer timer.Stop()
	for {
		select {
		case <-ticker.C:
			kv.mu.Lock()
			exist := kv.findHistoryData(args.ConfigNum, args.ShardNum)
			kv.mu.Unlock()
			if !exist {
				reply.Success = true
				return
			}
		case <-timer.C:
			return
		}
	}
}

func (kv *ShardKV) waitOP(op Op) (res NotifyMsg) {
	kv.mu.Lock()
	if op.ConfigNum == 0 || op.ConfigNum < kv.config.Num {
		res.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	if _, _, isLeader := kv.rf.Start(op); !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	ch := make(chan NotifyMsg, 1)
	kv.mu.Lock()
	kv.notifyCh[op.ReqID] = ch
	kv.mu.Unlock()

	t := time.NewTimer(OperationTimeout)
	defer t.Stop()
	select {
	case res = <-ch:
		kv.mu.Lock()
		delete(kv.notifyCh, op.ReqID)
		kv.mu.Unlock()
		return
	case <-t.C:
		kv.mu.Lock()
		delete(kv.notifyCh, op.ReqID)
		kv.mu.Unlock()
		res.Err = ErrTimeOut
		return
	}
}
