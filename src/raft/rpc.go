package raft

import "log"

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

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.VoteGranted = false

	defer rf.persist()

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
		if rf.logEntries[rf.getRelativeIdx(args.PreLogIndex)].Term == args.PreLogTerm {
			reply.Success = true
			rf.logEntries = append(rf.logEntries[:rf.getRelativeIdx(args.PreLogIndex)+1], args.Entries...)
			_, idx := rf.getLastLogTermIndex()
			reply.NextIndex = idx + 1
		} else {
			// 这里如果 term 不相等只有可能是 worker 的 term 更小
			reply.Success = false
			idx := args.PreLogIndex
			term := rf.logEntries[rf.getRelativeIdx(idx)].Term
			for idx > rf.commitIndex && idx > rf.lastSnapshotIndex && rf.logEntries[rf.getRelativeIdx(idx)].Term == term {
				idx--
			}
			rf.logEntries = rf.logEntries[:rf.getRelativeIdx(idx)+1]
			reply.NextIndex = idx + 1
		}
	} else if args.PreLogIndex > lastLogIndex {
		reply.Success = false
		reply.NextIndex = lastLogIndex + 1
	} else {
		idx := args.PreLogIndex + 1
		newLogIdx := 0
		if len(args.Entries) == 0 {
			// worker 比 master entry 多
			rf.logEntries = rf.logEntries[:rf.getRelativeIdx(idx)+1]
			reply.Success = false
			if rf.logEntries[rf.getRelativeIdx(args.PreLogIndex)].Term == args.PreLogTerm {
				reply.NextIndex = args.PreLogIndex + 1
			} else {
				term := rf.logEntries[rf.getRelativeIdx(args.PreLogIndex)].Term
				for idx > rf.commitIndex && idx > rf.lastSnapshotIndex {
					if rf.logEntries[rf.getRelativeIdx(idx)].Term != term {
						break
					}
					idx--
				}
				rf.logEntries = rf.logEntries[:rf.getRelativeIdx(idx)+1]
			}
		} else {
			if args.PreLogIndex < rf.lastSnapshotIndex {
				reply.Success = false
				reply.NextIndex = rf.lastSnapshotIndex + 1
			} else {
				// 比较 args 中的 entry 和自己的是否相同
				reply.Success = true
				//log.Printf("%v idx %v lastIdx %v lastSnap %v logs %v", rf.me, idx, lastLogIndex, rf.lastSnapshotIndex, rf.logEntries)
				for idx <= lastLogIndex && newLogIdx < len(args.Entries) {
					if rf.logEntries[rf.getRelativeIdx(idx)].Term != args.Entries[newLogIdx].Term {
						rf.logEntries = rf.logEntries[:rf.getRelativeIdx(idx)]
						reply.Success = false
						reply.NextIndex = idx
						break
					}
					idx++
					newLogIdx++
				}
				if reply.Success {
					if newLogIdx < len(args.Entries) {
						// 比较完成之后 args 中还剩有 entry
						rf.logEntries = append(rf.logEntries[:rf.getRelativeIdx(idx)], args.Entries[newLogIdx:]...)
						_, idx := rf.getLastLogTermIndex()
						reply.NextIndex = idx + 1
					} else {
						reply.NextIndex = idx
					}
				}
			}
		}
	}

	if reply.Success {
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			rf.notifyApplyCh <- struct{}{}
		}
	}

	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	if args.Term < rf.term {
		return
	}

	if args.Term > rf.term || rf.role != Follower {
		rf.term = args.Term
		rf.changeRole(Follower)
		rf.resetElectionTimer()
		defer rf.persist()
	}

	if rf.lastSnapshotIndex >= args.LastIncludedIndex {
		return
	}

	start := args.LastIncludedIndex - rf.lastSnapshotIndex
	if start < 0 {
		log.Fatal("install snapshot failed: unexpect index")
	} else if start >= len(rf.logEntries) {
		rf.logEntries = make([]LogEntry, 1)
		rf.logEntries[0].Term = args.LastIncludedTerm
	} else {
		rf.logEntries = rf.logEntries[start:]
	}

	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm

	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)
}
