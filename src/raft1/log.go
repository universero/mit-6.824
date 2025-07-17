package raft

import (
	"time"
)

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

// AppendEntriesArgs 添加日志条目或心跳
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XIndex  int
	XTerm   int
	XLen    int
}

func (rf *Raft) SendHeartbeat() {
	for !rf.killed() {
		rf.cond.L.Lock()
		for rf.role != leader {
			rf.cond.Wait()
		}
		rf.cond.L.Unlock()

		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			go rf.SendLog(peer)
		}
		time.Sleep(time.Duration(rf.heartbeatInterval) * time.Millisecond)
	}
}

func (rf *Raft) ReplicateLog(peer int) {
	for !rf.killed() {
		rf.peerCond[peer].L.Lock()
		rf.cond.L.Lock()
		for !(rf.role == leader && rf.log[len(rf.log)-1].Index >= rf.nextIndex[peer]) {
			rf.cond.L.Unlock()
			rf.peerCond[peer].Wait()
			rf.cond.L.Lock()
		}
		rf.cond.L.Unlock()
		rf.peerCond[peer].L.Unlock()

		rf.SendLog(peer)
	}
}

func (rf *Raft) SendLog(peer int) {
	rf.cond.L.Lock()
	if rf.nextIndex[peer] > rf.log[0].Index {
		args, reply := rf.SetAppendEntriesParams(peer)
		rf.cond.L.Unlock()
		rf.SendAppendEntries(peer, args, reply)
		return
	}
	// follower 日志太短, 发送snapshot
	args, reply := rf.SetInstallSnapshotParams(peer)
	rf.cond.L.Unlock()
	rf.SendInstallSnapshot(peer, args, reply)
}

func (rf *Raft) SetAppendEntriesParams(peer int) (*AppendEntriesArgs, *AppendEntriesReply) {
	prevIndex := rf.nextIndex[peer] - 1
	firstIndex := rf.log[0].Index
	prevIndexMem := prevIndex - firstIndex

	length := len(rf.log) - prevIndexMem - 1
	entries := make([]Entry, length)
	copy(entries, rf.log[prevIndexMem+1:])

	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  rf.log[prevIndexMem].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}, &AppendEntriesReply{}
}

func (rf *Raft) SendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.cond.L.Lock()
	if rf.role != leader {
		rf.cond.L.Unlock()
		return
	}
	rf.cond.L.Unlock()

	if !rf.peers[peer].Call("Raft.AppendEntries", args, reply) {
		DPrintf("[term %d] [server %d] meets a network failure when send AppendEntries RPC to peer %d\n", rf.currentTerm, rf.me, peer)
		return
	}
	rf.cond.L.Lock()
	DPrintf("[term %d] [server %d] receive a AppendEntries RPC response with term %d from peer %d\n%+v\n", rf.currentTerm, rf.me, reply.Term, peer, reply)
	defer rf.cond.L.Unlock()
	defer rf.persist()

	if args.Term > rf.currentTerm { // 新任期
		rf.role, rf.currentTerm, rf.votedFor = follower, reply.Term, -1
		rf.heartbeatTime = time.Now().UnixMilli()
		rf.ResetElectionTimeout()
		return
	}
	if reply.Term != rf.currentTerm || rf.role != leader {
		return
	}

	if !reply.Success {
		if reply.XLen != -1 {
			rf.nextIndex[peer] = reply.XLen
		} else {
			prevIndexMem := args.PrevLogIndex - rf.log[0].Index
			for i := prevIndexMem; i >= 0; i-- {
				if rf.log[i].Term == reply.XTerm {
					rf.nextIndex[peer] = i + 1 + rf.log[0].Index
					rf.peerCond[peer].Signal()
					return
				}
			}
			rf.nextIndex[peer] = reply.XIndex
		}
		if rf.nextIndex[peer] < 1 {
			rf.nextIndex[peer] = 1
		}
		rf.peerCond[peer].Signal()
		return
	}

	if len(args.Entries) > 0 {
		rf.nextIndex[peer] = args.Entries[len(args.Entries)-1].Index + 1
		rf.matchIndex[peer] = rf.nextIndex[peer] - 1
		DPrintf("[term %d] [server %d] find peer %d with nextIndex %d; matchIndex %d", rf.currentTerm, rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])
	}
	N := rf.log[len(rf.log)-1].Index
	for N > rf.commitIndex {
		count := 0
		for server := range rf.peers {
			if rf.matchIndex[server] >= N && rf.log[N-rf.log[0].Index].Term == rf.currentTerm {
				count += 1
			}
		}
		if count > len(rf.peers)/2 {
			break
		}
		N -= 1
	}
	rf.commitIndex = N
	rf.cond.Broadcast()
	if rf.role == leader && rf.log[len(rf.log)-1].Index >= rf.nextIndex[peer] {
		rf.peerCond[peer].Signal()
		return
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()
	defer rf.persist()
	DPrintf("[term %d] [server %d] receive a AppendEntries RPC Request with term %d \n%+v\n", rf.currentTerm, rf.me, args.Term, args)
	reply.Success, reply.XIndex, reply.XTerm, reply.XLen = false, -1, -1, -1

	if args.Term < rf.currentTerm { // 收到旧term的消息, 告知其新任期
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = args.Term
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm, rf.role = args.Term, follower
	rf.heartbeatTime = time.Now().UnixMilli()

	prevIndexMem := args.PrevLogIndex - rf.log[0].Index
	if prevIndexMem < 0 {
		return
	}
	if prevIndexMem >= len(rf.log) {
		reply.XLen = rf.log[len(rf.log)-1].Index
		return
	}

	if rf.log[prevIndexMem].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[prevIndexMem].Term
		for i := 0; i <= prevIndexMem; i++ {
			if rf.log[i].Term == reply.XTerm {
				reply.XIndex = i + rf.log[0].Index
				DPrintf("[term %d] [server %d] try to append entries but found conflict at index %d term %d", rf.currentTerm, rf.me, reply.XIndex, reply.XTerm)
				return
			}
		}
		return
	}

	count := 0
	for _, entry := range args.Entries {
		index, term := entry.Index, entry.Term
		indexMem := index - rf.log[0].Index
		if indexMem < len(rf.log) && term == rf.log[indexMem].Term {
			count++
			continue
		}
		if indexMem < len(rf.log) && term != rf.log[indexMem].Term {
			rf.log = rf.log[:indexMem]
			break
		}
	}
	rf.log = append(rf.log, args.Entries[count:]...)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		DPrintf("[term %d] [server %d] update commitIndex to %d", rf.currentTerm, rf.me, rf.commitIndex)
	}
	rf.cond.Broadcast()
	reply.Success = true
	return
}
