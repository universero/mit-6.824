package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) ResetElectionTimeout() {
	rf.electionInterval = 350 + (rand.Int63() % 200)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()
	defer rf.persist()

	reply.VoteGranted = false
	if rf.currentTerm > args.Term { // 旧term, 不予投票
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm { // 更新的任期, 转换为follower
		rf.currentTerm, rf.votedFor, rf.role = args.Term, -1, follower
		rf.heartbeatTime = time.Now().UnixMilli()
		rf.ResetElectionTimeout()
	}

	reply.Term = rf.currentTerm

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && ((args.LastLogTerm > rf.log[len(rf.log)-1].Term) ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term &&
			args.LastLogIndex >= rf.log[len(rf.log)-1].Index)) { // 已投给别人
		reply.VoteGranted = true
		rf.votedFor, rf.role, rf.heartbeatTime = args.CandidateId, follower, time.Now().UnixMilli()
		//DPrintf("[term %d] [server %d] vote for peer %d", rf.currentTerm, rf.me, args.CandidateId)
		return
	}
	//DPrintf("[term %d] [server %d] denied to vote to %d because of outdate log: lastest: %+v ; args: %+v", rf.currentTerm, rf.me, args.CandidateId, rf.log[len(rf.log)-1], args)
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, vote *int32) {
	reply := &RequestVoteReply{}
	if !rf.peers[server].Call("Raft.RequestVote", args, reply) {
		return
	}
	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()
	defer rf.persist()

	if reply.Term > rf.currentTerm { // 收到更新term
		rf.currentTerm, rf.role, rf.votedFor = reply.Term, follower, -1
		rf.heartbeatTime = time.Now().UnixMilli()
		rf.ResetElectionTimeout()
		return
	}
	if !reply.VoteGranted {
		return
	}
	if rf.role == candidate && rf.currentTerm == args.Term && atomic.AddInt32(vote, 1) > int32(len(rf.peers)/2) {
		//DPrintf("[term %d] [server %d] become leader", rf.currentTerm, rf.me)
		rf.role, rf.votedFor = leader, -1
		for peer := range rf.peers {
			rf.nextIndex[peer] = rf.log[len(rf.log)-1].Index + 1
			if peer == rf.me {
				rf.matchIndex[peer] = rf.nextIndex[peer] - 1
				continue
			}
			rf.matchIndex[peer] = 0
		}
		// 通知 SendHeartbeat 和 updateLog
		rf.cond.Broadcast()
	}
}

func (rf *Raft) StartElect() {
	for !rf.killed() {
		rf.cond.L.Lock()
		if time.Now().UnixMilli()-rf.heartbeatTime >= rf.electionInterval && rf.role != leader {
			//DPrintf("[term %d] [server %d] become candidate", rf.currentTerm, rf.me)
			rf.role, rf.votedFor = candidate, rf.me
			rf.currentTerm++
			rf.heartbeatTime = time.Now().UnixMilli()
			rf.ResetElectionTimeout()
			rf.persist()

			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.log[len(rf.log)-1].Index,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			var votes int32 = 1
			for peer := range rf.peers {
				if peer == rf.me {
					continue
				}
				go rf.sendRequestVote(peer, args, &votes)
			}
		}
		rf.cond.L.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}
