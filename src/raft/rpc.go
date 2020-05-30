package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.VoteGranted = false

	if args.Term < rf.term {
		return
	} else if args.Term == rf.term {
		// 我是 leader
		if rf.role == Leader {
			return
		}
		// 我已经投过票给你
		if rf.voteFor == args.CandidateID {
			reply.VoteGranted = true
			return
		}
		// 我已经投给了别人
		if rf.voteFor != -1 && rf.voteFor != args.CandidateID {
			return
		}
		// 不投票
	} else {
		rf.term = args.Term
		//rf.voteFor = -1
		rf.changeRole(Follower)
	}

	lastLogTerm, lastLogIndex := rf.getLastLogTermIndex()
	if lastLogTerm > args.LastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		// 选取限制
		return
	}

	rf.voteFor = args.CandidateID
	reply.VoteGranted = true
	rf.changeRole(Follower)
	rf.resetElectionTimer()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer DPrintf("worker %v commit index %v", rf.me, rf.commitIndex)

	reply.Term = rf.term
	if rf.term > args.Term {
		rf.mu.Unlock()
		return
	}

	rf.term = args.Term
	rf.changeRole(Follower)
	rf.resetElectionTimer()

	_, lastLogIndex := rf.getLastLogTermIndex()
	if args.PreLogIndex == lastLogIndex {
		if args.PreLogIndex < 0 || rf.logEntries[args.PreLogIndex].Term == args.PreLogTerm {
			reply.Success = true
			rf.logEntries = append(rf.logEntries[:args.PreLogIndex+1], args.Entries...)
			_, idx := rf.getLastLogTermIndex()
			reply.NextIndex = idx + 1
		} else {
			reply.Success = false
			idx := args.PreLogIndex
			term := rf.logEntries[idx].Term
			for idx > rf.commitIndex && rf.logEntries[idx].Term == term {
				idx -= 1
			}
			reply.NextIndex = idx + 1
		}
	} else if args.PreLogIndex > lastLogIndex {
		reply.Success = false
		reply.NextIndex = lastLogIndex + 1
	} else {
		reply.Success = false
		reply.NextIndex = args.PreLogIndex + 1
	}

	if reply.Success {
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		}
	}
	rf.mu.Unlock()
}
