package raft

import (
	"6.5840/raftapi"
	"time"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Offset            int  // lab要求不用offset, 所以默认0即可
	Done              bool // lab要求不用offset, 一次性传输, Done默认为true
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) SetInstallSnapshotParams(peer int) (*InstallSnapshotArgs, *InstallSnapshotReply) {
	return &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log[0].Index,
		LastIncludedTerm:  rf.log[0].Term,
		Data:              rf.persister.ReadSnapshot(),
		Offset:            0,
		Done:              true,
	}, &InstallSnapshotReply{}
}

func (rf *Raft) SendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.cond.L.Lock()
	if rf.role != leader {
		rf.cond.L.Unlock()
		return
	}
	rf.cond.L.Unlock()

	if !rf.peers[peer].Call("Raft.InstallSnapshot", args, reply) {
		return
	}

	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()
	if rf.role != leader {
		return
	}
	if reply.Term > rf.currentTerm { // 新任期
		rf.role, rf.currentTerm, rf.votedFor = follower, reply.Term, -1
		rf.heartbeatTime = time.Now().UnixMilli()
		rf.ResetElectionTimeout()
		return
	}
	if args.LastIncludedIndex != rf.log[0].Index {
		return
	}
	rf.nextIndex[peer] = args.LastIncludedIndex + 1
	rf.matchIndex[peer] = args.LastIncludedIndex
	//rf.persister.Save(rf.encodeState(), args.Data) // 保存快照
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()
	//DPrintf("[term %d] [server %d] receive a InstallSanpshot Request \n%+v\n", rf.currentTerm, rf.me, args)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.commitIndex >= args.LastIncludedIndex { // 已具备快照中的内容
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.heartbeatTime = time.Now().UnixMilli()
	if args.Term > rf.currentTerm { // 新term
		rf.votedFor = -1
	}
	rf.role, rf.currentTerm = follower, args.Term

	if !args.Done {
		panic("incompleted chuck") // 如果实现分块则应该在缓存下来等待完全传输
	}

	var indexMem = -1
	for i, entry := range rf.log { // 判断日志中是否有快照中的最后一条日志
		if entry.Index == args.LastIncludedIndex && entry.Term == args.LastIncludedTerm {
			indexMem = i
		}
	}
	if indexMem == -1 { // 日志中不包含最后一条日志
		rf.log = rf.log[:0]
		rf.log = append(rf.log, Entry{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm, Command: nil})
	} else {
		rf.log = append([]Entry{}, rf.log[indexMem:]...)
		rf.log[0].Command = nil
	}
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.persister.Save(rf.encodeState(), args.Data) // 保存快照
	rf.snapshot = &raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}
