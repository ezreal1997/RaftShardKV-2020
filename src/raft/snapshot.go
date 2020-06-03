package raft

import (
	"log"
	"time"
)

func (rf *Raft) SavePersistAndSnapshot(logIndex int, snapshotData []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if logIndex <= rf.lastSnapshotIndex {
		return
	}

	if logIndex > rf.commitIndex {
		log.Fatalf("logIndex %v > rf.commitdex %v", logIndex, rf.commitIndex)
	}

	lastLog := rf.logEntries[logIndex-rf.lastSnapshotIndex]

	rf.logEntries = rf.logEntries[rf.getRelativeIdx(logIndex):]
	rf.lastSnapshotIndex = logIndex
	rf.lastSnapshotTerm = lastLog.Term

	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshotData)
}

func (rf *Raft) sendInstallSnapshot(peerIdx int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.term,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	timer := time.NewTimer(RPCTimeout)
	defer timer.Stop()
	for {
		timer.Reset(RPCTimeout)
		okCh := make(chan bool, 1)
		var reply InstallSnapshotReply
		go func() {
			ok := rf.peers[peerIdx].Call("Raft.InstallSnapshot", &args, &reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			okCh <- ok
		}()
		select {
		case <-rf.stopCh:
			return
		case <-timer.C:
			continue
		case ok := <-okCh:
			if !ok {
				continue
			}
		}
		rf.mu.Lock()
		if rf.term != args.Term || rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.term {
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.term = reply.Term
			rf.persist()
			rf.mu.Unlock()
			return
		}
		if args.LastIncludedIndex > rf.matchIndex[peerIdx] {
			rf.matchIndex[peerIdx] = args.LastIncludedIndex
		}
		if args.LastIncludedIndex+1 > rf.nextIndex[peerIdx] {
			rf.nextIndex[peerIdx] = args.LastIncludedIndex + 1
		}
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) getRelativeIdx(logIndex int) int {
	idx := logIndex - rf.lastSnapshotIndex
	if idx < 0 {
		return -1
	} else {
		return idx
	}
}
