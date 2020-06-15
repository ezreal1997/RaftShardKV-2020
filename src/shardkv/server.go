package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"go.uber.org/multierr"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

const (
	OperationTimeout   = time.Millisecond * 500
	PullConfigInterval = time.Millisecond * 100
	PullShardsInterval = time.Millisecond * 200
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Operation string
	ClientID  int64
	MsgID     int64
	ReqID     int64
	ConfigNum int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	config        shardmaster.Config
	oldConfig     shardmaster.Config
	notifyCh      map[int64]chan NotifyMsg
	lastMsgIdx    [shardmaster.NShards]map[int64]int64
	ownShards     map[int]struct{}
	data          [shardmaster.NShards]map[string]string
	waitShardIDs  map[int]struct{}
	historyShards map[int]map[int]ShardData
	masterClient  *shardmaster.Clerk

	pullConfigTimer *time.Timer
	pullShardsTimer *time.Timer

	stopCh    chan struct{}
	persister *raft.Persister
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.stopCh)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := &ShardKV{
		mu:              sync.Mutex{},
		me:              me,
		applyCh:         make(chan raft.ApplyMsg),
		make_end:        make_end,
		gid:             gid,
		masters:         masters,
		maxraftstate:    maxraftstate,
		notifyCh:        make(map[int64]chan NotifyMsg),
		lastMsgIdx:      [10]map[int64]int64{},
		ownShards:       make(map[int]struct{}),
		data:            [shardmaster.NShards]map[string]string{},
		waitShardIDs:    make(map[int]struct{}),
		historyShards:   make(map[int]map[int]ShardData),
		stopCh:          make(chan struct{}),
		persister:       persister,
		pullConfigTimer: time.NewTimer(PullConfigInterval),
		pullShardsTimer: time.NewTimer(PullShardsInterval),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.masterClient = shardmaster.MakeClerk(kv.masters)

	for i := range kv.data {
		kv.data[i] = make(map[string]string)
	}
	for i := range kv.lastMsgIdx {
		kv.lastMsgIdx[i] = make(map[int64]int64)
	}

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	go kv.applyLoop()
	go kv.updateConfigLoop()
	go kv.getShardLoop()

	return kv
}

func (kv *ShardKV) applyLoop() {
	for {
		select {
		case <-kv.stopCh:
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				kv.readSnapShotData(kv.persister.ReadSnapshot())
				continue
			}
			if op, ok := msg.Command.(Op); ok {
				kv.applyOp(msg, op)
			} else if config, ok := msg.Command.(shardmaster.Config); ok {
				kv.applyConfig(msg, config)
			} else if mergeData, ok := msg.Command.(ShardData); ok {
				kv.applyShardData(msg, mergeData)
			} else if cleanUp, ok := msg.Command.(DeleteShardDataArgs); ok {
				kv.applyCleanUp(msg, cleanUp)
			} else {
				log.Fatalf("apply failed: invalid apply msg %v", msg)
			}
		}
	}
}

func (kv *ShardKV) updateConfigLoop() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.pullConfigTimer.C:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.pullConfigTimer.Reset(PullConfigInterval)
				break
			}
			kv.mu.Lock()
			lastConfigNum := kv.config.Num
			kv.mu.Unlock()

			config := kv.masterClient.Query(lastConfigNum + 1)
			if config.Num == lastConfigNum+1 {
				kv.mu.Lock()
				if len(kv.waitShardIDs) == 0 && kv.config.Num+1 == config.Num {
					kv.mu.Unlock()
					kv.rf.Start(config.Duplicate())
				} else {
					kv.mu.Unlock()
				}
			}
			kv.pullConfigTimer.Reset(PullConfigInterval)
		}
	}
}

func (kv *ShardKV) getShardLoop() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.pullShardsTimer.C:
			_, isLeader := kv.rf.GetState()
			if isLeader {
				kv.mu.Lock()
				for shardID := range kv.waitShardIDs {
					go kv.pullShard(shardID, kv.oldConfig)
				}
				kv.mu.Unlock()
			}
			kv.pullShardsTimer.Reset(PullShardsInterval)
		}
	}
}

func (kv *ShardKV) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	kv.rf.SavePersistAndSnapshot(logIndex, kv.genSnapshotData())
}

func (kv *ShardKV) genSnapshotData() []byte {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)

	if err := multierr.Combine(
		enc.Encode(kv.data),
		enc.Encode(kv.lastMsgIdx),
		enc.Encode(kv.waitShardIDs),
		enc.Encode(kv.historyShards),
		enc.Encode(kv.config),
		enc.Encode(kv.oldConfig),
		enc.Encode(kv.ownShards),
	); err != nil {
		log.Fatalf("encode snapshot data failed: %v", err)
	}

	return buf.Bytes()
}

func (kv *ShardKV) readSnapShotData(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)

	var (
		kvs           [shardmaster.NShards]map[string]string
		lastMsgIdx    [shardmaster.NShards]map[int64]int64
		waitShardIDs  map[int]struct{}
		historyShards map[int]map[int]ShardData
		config        shardmaster.Config
		oldConfig     shardmaster.Config
		ownShards     map[int]struct{}
	)

	if err := multierr.Combine(
		dec.Decode(&kvs),
		dec.Decode(&lastMsgIdx),
		dec.Decode(&waitShardIDs),
		dec.Decode(&historyShards),
		dec.Decode(&config),
		dec.Decode(&oldConfig),
		dec.Decode(&ownShards),
	); err != nil {
		log.Fatalf("decode snapshot data failed: %v", err)
	} else {
		kv.data = kvs
		kv.lastMsgIdx = lastMsgIdx
		kv.waitShardIDs = waitShardIDs
		kv.historyShards = historyShards
		kv.config = config
		kv.oldConfig = oldConfig
		kv.ownShards = ownShards
	}
}

func (kv *ShardKV) applyOp(msg raft.ApplyMsg, op Op) {
	kv.mu.Lock()

	shardID := key2shard(op.Key)
	var isRepeated bool
	if val, ok := kv.lastMsgIdx[shardID][op.ClientID]; ok {
		isRepeated = val == op.MsgID
	} else {
		isRepeated = false
	}
	var isWrongGroup bool
	if op.ConfigNum == 0 || op.ConfigNum != kv.config.Num {
		isWrongGroup = true
	} else if _, ok := kv.ownShards[shardID]; !ok {
		isWrongGroup = true
	} else if _, ok := kv.waitShardIDs[shardID]; ok {
		isWrongGroup = true
	} else {
		isWrongGroup = false
	}

	if isWrongGroup {
		if ch, ok := kv.notifyCh[op.ReqID]; ok {
			ch <- NotifyMsg{Err: ErrWrongGroup}
		}
		kv.mu.Unlock()
		return
	}
	switch op.Operation {
	case "Put":
		if !isRepeated {
			kv.data[shardID][op.Key] = op.Value
			kv.lastMsgIdx[shardID][op.ClientID] = op.MsgID
		}
	case "Append":
		if !isRepeated {
			_, v := kv.getData(op.Key)
			kv.data[shardID][op.Key] = v + op.Value
			kv.lastMsgIdx[shardID][op.ClientID] = op.MsgID
		}

	case "Get":
	default:
		log.Fatalf(fmt.Sprintf("invalid method: %s", op.Operation))
	}
	kv.saveSnapshot(msg.CommandIndex)
	if ch, ok := kv.notifyCh[op.ReqID]; ok {
		m := NotifyMsg{Err: OK}
		if op.Operation == "Get" {
			m.Err, m.Value = kv.getData(op.Key)
		}
		ch <- m
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) getData(key string) (err Err, val string) {
	if v, ok := kv.data[key2shard(key)][key]; ok {
		err = OK
		val = v
		return
	} else {
		err = ErrNoKey
		return err, ""
	}
}

func (kv *ShardKV) applyConfig(msg raft.ApplyMsg, config shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if config.Num <= kv.config.Num {
		kv.saveSnapshot(msg.CommandIndex)
		return
	}
	if config.Num != kv.config.Num+1 {
		log.Fatalf("applyConfig failed: config num %v != %v + 1", config.Num, kv.config.Num)
	}

	oldConfig := kv.config.Duplicate()

	deleteShardIDs := make([]int, 0, shardmaster.NShards)
	ownShardIDs := make([]int, 0, shardmaster.NShards)
	newShardIDs := make([]int, 0, shardmaster.NShards)

	for i := 0; i < shardmaster.NShards; i++ {
		if config.Shards[i] == kv.gid {
			ownShardIDs = append(ownShardIDs, i)
			if oldConfig.Shards[i] != kv.gid {
				newShardIDs = append(newShardIDs, i)
			}
		} else {
			if oldConfig.Shards[i] == kv.gid {
				deleteShardIDs = append(deleteShardIDs, i)
			}
		}
	}

	historyShards := make(map[int]ShardData)
	for _, shardID := range deleteShardIDs {
		mergeShardData := ShardData{
			ConfigNum:  oldConfig.Num,
			ShardNum:   shardID,
			Data:       kv.data[shardID],
			MsgIndexes: kv.lastMsgIdx[shardID],
		}
		historyShards[shardID] = mergeShardData
		kv.data[shardID] = make(map[string]string)
		kv.lastMsgIdx[shardID] = make(map[int64]int64)
	}
	kv.historyShards[oldConfig.Num] = historyShards

	kv.ownShards = make(map[int]struct{})
	for _, shardId := range ownShardIDs {
		kv.ownShards[shardId] = struct{}{}
	}
	kv.waitShardIDs = make(map[int]struct{})
	if oldConfig.Num != 0 {
		for _, shardId := range newShardIDs {
			kv.waitShardIDs[shardId] = struct{}{}
		}
	}
	kv.config = config.Duplicate()
	kv.oldConfig = oldConfig
	kv.saveSnapshot(msg.CommandIndex)
}

func (kv *ShardKV) applyShardData(msg raft.ApplyMsg, data ShardData) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.saveSnapshot(msg.CommandIndex)

	if kv.config.Num != data.ConfigNum+1 {
		return
	}
	if _, ok := kv.waitShardIDs[data.ShardNum]; !ok {
		return
	}
	kv.data[data.ShardNum] = make(map[string]string)
	kv.lastMsgIdx[data.ShardNum] = make(map[int64]int64)
	for k, v := range data.Data {
		kv.data[data.ShardNum][k] = v
	}
	for k, v := range data.MsgIndexes {
		kv.lastMsgIdx[data.ShardNum][k] = v
	}
	delete(kv.waitShardIDs, data.ShardNum)
	go kv.sendDeleteShardDataRequest(kv.oldConfig, data.ShardNum)
}

func (kv *ShardKV) applyCleanUp(msg raft.ApplyMsg, data DeleteShardDataArgs) {
	kv.mu.Lock()
	if kv.findHistoryData(data.ConfigNum, data.ShardNum) {
		delete(kv.historyShards[data.ConfigNum], data.ShardNum)
	}
	kv.saveSnapshot(msg.CommandIndex)
	kv.mu.Unlock()
}

func (kv *ShardKV) sendDeleteShardDataRequest(config shardmaster.Config, shardID int) {
	args := &DeleteShardDataArgs{
		ConfigNum: config.Num,
		ShardNum:  shardID,
	}

	t := time.NewTimer(time.Millisecond * 500)
	defer t.Stop()
	for {
		for _, s := range config.Groups[config.Shards[shardID]] {
			var reply DeleteShardDataReply
			client := kv.make_end(s)
			doneCh, done := make(chan bool, 1), false

			go func(args *DeleteShardDataArgs, reply *DeleteShardDataReply) {
				doneCh <- client.Call("ShardKV.DeleteShardData", args, reply)
			}(args, &reply)

			t.Reset(time.Millisecond * 500)
			select {
			case <-kv.stopCh:
				return
			case <-t.C:
			case done = <-doneCh:
			}
			if done && reply.Success {
				return
			}
		}
		kv.mu.Lock()
		if kv.config.Num != config.Num+1 || len(kv.waitShardIDs) == 0 {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) findHistoryData(configNum int, shardID int) bool {
	if _, ok := kv.historyShards[configNum]; ok {
		if _, ok := kv.historyShards[configNum][shardID]; ok {
			return true
		}
	}
	return false
}

func (kv *ShardKV) pullShard(shardID int, config shardmaster.Config) {
	args := FetchShardDataArgs{
		ConfigNum: config.Num,
		ShardNum:  shardID,
	}
	for _, s := range config.Groups[config.Shards[shardID]] {
		client := kv.make_end(s)
		var reply FetchShardDataReply
		if ok := client.Call("ShardKV.FetchShardData", &args, &reply); ok {
			if reply.Success {
				kv.mu.Lock()
				if _, ok = kv.waitShardIDs[shardID]; ok && kv.config.Num == config.Num+1 {
					replyCopy := reply.Duplicate()
					shardDataArgs := ShardData{
						ConfigNum:  args.ConfigNum,
						ShardNum:   args.ShardNum,
						Data:       replyCopy.Data,
						MsgIndexes: replyCopy.MsgIndexes,
					}
					kv.mu.Unlock()
					_, _, isLeader := kv.rf.Start(shardDataArgs)
					if !isLeader {
						break
					}
				} else {
					kv.mu.Unlock()
				}
			}
		}
	}
}
