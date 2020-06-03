package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"../labrpc"
)

const ChangeLeaderInterval = time.Millisecond * 20

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID int64
	leaderID int
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
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) genMsgID() msgID {
	return msgID(nrand())
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key, MsgID: ck.genMsgID(), ClientID: ck.clientID}
	leaderID := ck.leaderID
	for {
		var reply GetReply
		if ok := ck.servers[leaderID].Call("KVServer.Get", &args, &reply); !ok {
			time.Sleep(ChangeLeaderInterval)
			leaderID = (leaderID + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
			ck.leaderID = leaderID
			return reply.Value
		case ErrNoKey:
			ck.leaderID = leaderID
			return ""
		case ErrTimeOut:
			continue
		default:
			time.Sleep(ChangeLeaderInterval)
			leaderID = (leaderID + 1) % len(ck.servers)
			continue
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		MsgID:    ck.genMsgID(),
		ClientID: ck.clientID,
	}
	leaderID := ck.leaderID
	for {
		var reply PutAppendReply
		ok := ck.servers[leaderID].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			time.Sleep(ChangeLeaderInterval)
			leaderID = (leaderID + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
			return
		case ErrNoKey:
			log.Fatalf("upexpected client putAppend error: %v", reply.Err)
		case ErrWrongLeader:
			time.Sleep(ChangeLeaderInterval)
			leaderID = (leaderID + 1) % len(ck.servers)
			continue
		case ErrTimeOut:
			continue
		default:
			log.Fatalf("unknown putAppend error: %v", reply.Err)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
