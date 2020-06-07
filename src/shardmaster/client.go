package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientID int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:  servers,
		clientID: nrand(),
	}

	return ck
}

func (ck *Clerk) genMsgID() msgID {
	return msgID(nrand())
}

func (ck *Clerk) Query(configNum int) Config {
	args := &QueryArgs{
		CommonArgs: CommonArgs{
			MsgID:    ck.genMsgID(),
			ClientID: ck.clientID,
		},
		Num: configNum,
	}
	for ; ; time.Sleep(100 * time.Millisecond) {
		for _, s := range ck.servers {
			var reply QueryReply
			ok := s.Call("ShardMaster.Query", args, &reply)
			if ok && !reply.WrongLeader {
				return reply.Config
			}
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		CommonArgs: CommonArgs{
			MsgID:    ck.genMsgID(),
			ClientID: ck.clientID,
		},
		Servers: servers,
	}
	for ; ; time.Sleep(100 * time.Millisecond) {
		for _, s := range ck.servers {
			var reply JoinReply
			ok := s.Call("ShardMaster.Join", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		CommonArgs: CommonArgs{
			MsgID:    ck.genMsgID(),
			ClientID: ck.clientID,
		},
		GIDs: gids,
	}
	for ; ; time.Sleep(100 * time.Millisecond) {
		for _, s := range ck.servers {
			var reply LeaveReply
			ok := s.Call("ShardMaster.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		CommonArgs: CommonArgs{
			MsgID:    ck.genMsgID(),
			ClientID: ck.clientID,
		},
		Shard: shard,
		GID:   gid,
	}
	for ; ; time.Sleep(100 * time.Millisecond) {
		for _, s := range ck.servers {
			var reply MoveReply
			ok := s.Call("ShardMaster.Move", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
	}
}
