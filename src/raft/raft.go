package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const (
	ElectionTimeout      = time.Millisecond * 300
	AppendEntriesTimeout = time.Millisecond * 150
	RPCTimeout           = time.Millisecond * 100
	ApplyInterval        = time.Millisecond * 100
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role Role
	term int

	electionTimer       *time.Timer
	appendEntriesTimers []*time.Timer
	applyTimer          *time.Timer

	notifyApplyCh chan struct{}
	stopCh        chan struct{}

	voteFor     int
	logEntries  []LogEntry
	applyCh     chan ApplyMsg
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.term, rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	reqTimer := time.NewTimer(RPCTimeout)
	defer reqTimer.Stop()
	for ; ; time.Sleep(10 * time.Millisecond) {
		var r RequestVoteReply
		ch := make(chan bool, 1)
		go func() {
			ok := rf.peers[server].Call("Raft.RequestVote", args, &r)
			ch <- ok
		}()
		select {
		case <-reqTimer.C:
			return
		case ok := <-ch:
			if !ok {
				continue
			}
			reply.Term = r.Term
			reply.VoteGranted = r.VoteGranted
			return

		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	_, lastIndex := rf.getLastLogTermIndex()
	index, term, isLeader := lastIndex+1, rf.term, rf.role == Leader

	if isLeader {
		rf.logEntries = append(rf.logEntries, LogEntry{
			Term:    rf.term,
			Command: command,
		})
		rf.matchIndex[rf.me] = index
	}
	rf.resetAppendEntriesTimers()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.stopCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:                  sync.Mutex{},
		peers:               peers,
		persister:           persister,
		me:                  me,
		applyCh:             applyCh,
		term:                0,
		voteFor:             -1,
		role:                Follower,
		logEntries:          make([]LogEntry, 1),
		commitIndex:         0,
		nextIndex:           make([]int, len(peers)),
		matchIndex:          make([]int, len(peers)),
		electionTimer:       time.NewTimer(randElectionTimeout()),
		appendEntriesTimers: make([]*time.Timer, len(peers)),
		applyTimer:          time.NewTimer(ApplyInterval),
		notifyApplyCh:       make(chan struct{}, 100),
		stopCh:              make(chan struct{}),
		lastApplied:         0,
	}
	for i := range rf.peers {
		rf.appendEntriesTimers[i] = time.NewTimer(AppendEntriesTimeout)
		rf.nextIndex[i] = 1
	}

	// Your initialization code here (2A, 2B, 2C).
	// apply log
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.applyTimer.C:
				rf.notifyApplyCh <- struct{}{}
			case <-rf.notifyApplyCh:
				rf.startApplyLogs()
			}
		}
	}()
	// 发起投票
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.electionTimer.C:
				rf.startElection()
			}
		}
	}()
	// leader 发送日志
	for i := range peers {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			for {
				select {
				case <-rf.stopCh:
					return
				case <-rf.appendEntriesTimers[idx].C:
					rf.appendEntriesToPeer(idx)
				}
			}
		}(i)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) getLastLogTermIndex() (int, int) {
	return rf.logEntries[len(rf.logEntries)-1].Term, len(rf.logEntries) - 1
}

func (rf *Raft) changeRole(role Role) {
	rf.role = role
	switch role {
	case Follower:
	case Candidate:
		rf.term += 1
		rf.voteFor = rf.me
		rf.resetElectionTimer()
	case Leader:
		_, lastLogIndex := rf.getLastLogTermIndex()
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = lastLogIndex
		rf.resetElectionTimer()
	default:
		panic("unknown role")
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randElectionTimeout())
}

func randElectionTimeout() time.Duration {
	return ElectionTimeout + time.Duration(rand.Int63())%ElectionTimeout
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.electionTimer.Reset(randElectionTimeout())
	if rf.role == Leader {
		rf.mu.Unlock()
		return
	}
	rf.changeRole(Candidate)
	lastLogTerm, lastLogIndex := rf.getLastLogTermIndex()
	args := RequestVoteArgs{
		Term:         rf.term,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	resCount, grantedCount := 1, 1
	votesCh := make(chan bool, len(rf.peers))

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(idx int) {
			var reply RequestVoteReply
			rf.sendRequestVote(idx, &args, &reply)
			votesCh <- reply.VoteGranted
			if reply.Term > args.Term {
				rf.mu.Lock()
				if rf.term < reply.Term {
					rf.term = reply.Term
					rf.changeRole(Follower)
					rf.resetElectionTimer()
				}
				rf.mu.Unlock()
			}
		}(idx)
	}

	for {
		r := <-votesCh
		resCount++
		if r {
			grantedCount++
		}
		if resCount == len(rf.peers) || grantedCount > len(rf.peers)/2 || resCount-grantedCount > len(rf.peers)/2 {
			break
		}
	}

	if grantedCount <= len(rf.peers)/2 {
		return
	}

	rf.mu.Lock()
	if rf.term == args.Term && rf.role == Candidate {
		rf.changeRole(Leader)
	}
	if rf.role == Leader {
		rf.resetAppendEntriesTimers()
	}
	rf.mu.Unlock()
}

func (rf *Raft) resetAppendEntriesTimers() {
	for i, _ := range rf.appendEntriesTimers {
		rf.appendEntriesTimers[i].Stop()
		rf.appendEntriesTimers[i].Reset(0)
	}
}

func (rf *Raft) resetAppendEntriesTimer(peerIdx int) {
	rf.appendEntriesTimers[peerIdx].Stop()
	rf.appendEntriesTimers[peerIdx].Reset(AppendEntriesTimeout)
}

func (rf *Raft) appendEntriesToPeer(peerIdx int) {
	reqTimer := time.NewTimer(RPCTimeout)
	defer reqTimer.Stop()
	for ; ; time.Sleep(10 * time.Millisecond) {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.resetAppendEntriesTimer(peerIdx)
			rf.mu.Unlock()
			return
		}
		args := rf.getAppendEntriesArgs(peerIdx)
		DPrintf("master %v to worker %v args %+v", rf.me, peerIdx, args)
		rf.resetAppendEntriesTimer(peerIdx)
		rf.mu.Unlock()

		var reply AppendEntriesReply
		ch := make(chan bool, 1)

		go func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ch <- rf.peers[peerIdx].Call("Raft.AppendEntries", args, reply)
		}(&args, &reply)

		select {
		case <-reqTimer.C:
			return
		case ok := <-ch:
			if !ok {
				continue
			}
		}

		rf.mu.Lock()
		if reply.Term > rf.term {
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.term = reply.Term
			rf.mu.Unlock()
			return
		}

		if rf.role != Leader || rf.term != args.Term {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			if reply.NextIndex > rf.nextIndex[peerIdx] {
				rf.nextIndex[peerIdx] = reply.NextIndex
				rf.matchIndex[peerIdx] = reply.NextIndex - 1
			}
			if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.term {
				// 只 commit 自己 term 的 index
				rf.updateCommitIndex()
			}
			DPrintf("success master %v to worker %v entry %v commit %v next %v match %v", rf.me, peerIdx, rf.logEntries, rf.commitIndex, rf.nextIndex, rf.matchIndex)
			rf.mu.Unlock()
			return
		}

		DPrintf("fail master %v to worker %v reply %+v", rf.me, peerIdx, reply)

		if reply.NextIndex > 0 {
			rf.nextIndex[peerIdx] = reply.NextIndex
			rf.mu.Unlock()
			continue
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) getAppendEntriesArgs(peerIdx int) AppendEntriesArgs {
	preLogIndex, preLogTerm, logs := rf.getAppendLogs(peerIdx)
	args := AppendEntriesArgs{
		Term:         rf.term,
		LeaderID:     rf.me,
		PreLogIndex:  preLogIndex,
		PreLogTerm:   preLogTerm,
		Entries:      logs,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) getAppendLogs(peerIdx int) (preLogIndex, preLogTerm int, res []LogEntry) {
	nextIdx := rf.nextIndex[peerIdx]
	lastLogTerm, lastLogIndex := rf.getLastLogTermIndex()

	if nextIdx > lastLogIndex {
		// 没有需要发送的 log
		preLogIndex = lastLogIndex
		preLogTerm = lastLogTerm
		return
	}

	res = append(make([]LogEntry, 0), rf.logEntries[nextIdx:]...)
	preLogIndex = nextIdx - 1
	if preLogIndex < 0 {
		preLogTerm = -1
	} else {
		preLogTerm = rf.logEntries[preLogIndex].Term
	}
	return
}

func (rf *Raft) updateCommitIndex() {
	hasCommit := false
	for i := rf.commitIndex + 1; i < len(rf.logEntries); i++ {
		count := 0
		for _, m := range rf.matchIndex {
			if m >= i {
				count++
				if count > len(rf.peers)/2 {
					hasCommit = true
					rf.commitIndex = i
					break
				}
			}
		}
		if rf.commitIndex != i {
			// index i 没有 commit, 结束 commit
			break
		}
	}
	if hasCommit {
		rf.notifyApplyCh <- struct{}{}
	}
}

func (rf *Raft) startApplyLogs() {
	defer rf.applyTimer.Reset(ApplyInterval)
	rf.mu.Lock()

	if rf.commitIndex <= rf.lastApplied {
		rf.mu.Unlock()
		return
	}

	msg := make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg = append(msg, ApplyMsg{
			CommandValid: true,
			Command:      rf.logEntries[i].Command,
			CommandIndex: i,
		})
	}
	rf.mu.Unlock()

	for _, m := range msg {
		rf.applyCh <- m
		rf.mu.Lock()
		rf.lastApplied = m.CommandIndex
		rf.mu.Unlock()
	}
}
