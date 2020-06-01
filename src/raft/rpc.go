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
		DPrintf("worker %v 1", rf.me)
		if rf.logEntries[args.PreLogIndex].Term == args.PreLogTerm {
			reply.Success = true
			rf.logEntries = append(rf.logEntries[:args.PreLogIndex+1], args.Entries...)
			_, idx := rf.getLastLogTermIndex()
			reply.NextIndex = idx + 1
		} else {
			reply.Success = false
			idx := args.PreLogIndex
			term := rf.logEntries[idx].Term
			DPrintf("worker %v 1 idx %v term %v commit %v", rf.me, idx, term, rf.commitIndex)
			for idx > rf.commitIndex && rf.logEntries[idx].Term == term {
				idx -= 1
			}
			DPrintf("worker %v 1 idx %v term %v commit %v", rf.me, idx, term, rf.commitIndex)
			rf.logEntries = rf.logEntries[:idx+1]
			reply.NextIndex = idx + 1
		}
	} else if args.PreLogIndex > lastLogIndex {
		DPrintf("worker %v 2", rf.me)
		reply.Success = false
		reply.NextIndex = lastLogIndex + 1
	} else {
		DPrintf("worker %v 3", rf.me)
		idx := args.PreLogIndex
		newLogIdx := 0
		if len(args.Entries) == 0 {
			rf.logEntries = rf.logEntries[:args.PreLogIndex+1]
			reply.Success = false
			if rf.logEntries[args.PreLogIndex].Term == args.PreLogTerm {
				reply.NextIndex = args.PreLogIndex + 1
			} else {
				term := rf.logEntries[args.PreLogIndex].Term
				for idx >= rf.commitIndex {
					if rf.logEntries[idx].Term != term {
						break
					}
					idx--
				}
				rf.logEntries = rf.logEntries[:idx+1]
			}
		} else {
			reply.Success = true
			for idx <= lastLogIndex && newLogIdx < len(args.Entries) {
				if rf.logEntries[idx].Term != args.Entries[newLogIdx].Term {
					rf.logEntries = rf.logEntries[:idx]
					reply.Success = false
					reply.NextIndex = idx
					break
				}
				idx++
				newLogIdx++
			}
			if reply.Success {
				if newLogIdx < len(args.Entries) {
					rf.logEntries = append(rf.logEntries[:idx], args.Entries[newLogIdx:]...)
					_, idx := rf.getLastLogTermIndex()
					reply.NextIndex = idx + 1
				} else {
					reply.NextIndex = idx
				}
			}
		}
	}

	if reply.Success {
		if rf.commitIndex < args.LeaderCommit {
			_, idx := rf.getLastLogTermIndex()
			if idx > args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = idx
			}
			rf.notifyApplyCh <- struct{}{}
		}
	}

	DPrintf("worker %v entry %v commit %v next %v match %v", rf.me, rf.logEntries, rf.commitIndex, rf.nextIndex, rf.matchIndex)
	rf.mu.Unlock()
}
