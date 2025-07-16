package raft2

import (
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

const (
	follower  = "follower"
	candidate = "candidate"
	leader    = "leader"
)

var heartbeatGap = time.Duration(100) * time.Millisecond

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *tester.Persister   // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	currentTerm int                 // 当前任期号(初始0), 持久化
	votedFor    int                 // 已投票对象(初始-1), 持久化
	log         []Log               // 日志条目, 持久化
	commitIndex int                 // 已提交的最大日志索引, 易失
	lastApplied int                 // 已应用的最大日志索引, 易失
	nextIndex   []int               // 每个server对应的下一个日志条目索引, leader
	matchIndex  []int               // 每个server对应的已复制的最大索引, leader
	role        string              // 0 follower, 1 candidate, 2 leader
	idle        bool                // 是否收到过leader的RPC或candidate的GrantVote
	leaderId    int                 // 此时leaderId
	timer       *time.Timer         // 计时器
}

type Log struct {
	Term int // 日志对应任期
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader := rf.currentTerm, rf.role == leader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// PersistBytes how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // 候选者任期
	CandidateId  int // 候选者Id
	LastLogIndex int // 候选者最后一条的日志的索引
	LastLogTerm  int // 候选者最后一条日志的任期
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()

	// 收到了更旧任期的请求, 不投票并通知更新当前任期
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 收到了更新的任期
	rf.mu.Unlock()
	rf.convertToFollower(args.Term, -1)
	reply.Term = rf.currentTerm

	// TODO: 日志相关校验
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = false
		return
	}
	DPrintf("[term %d] [server %d] vote for peer %d", rf.currentTerm, rf.me, args.CandidateId)
	reply.VoteGranted = true       // 确认投票
	rf.votedFor = args.CandidateId // 记录投票对象
	return
}

// AppendEntriesArgs 添加日志条目或心跳
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 新任期或者当前任期有了leader, 转换为follower
	rf.convertToFollower(args.Term, args.LeaderId)
	DPrintf("[term %d] [server %d] receive the heartbeat from leader %d", rf.currentTerm, rf.me, rf.leaderId)

	// TODO 日志相关
	// TODO 实际处理条目
	return
}

// 发送AppendEntries请求, 闲时作为心跳请求WW
func (rf *Raft) appendEntries() {
	rf.mu.Lock()
	term := rf.currentTerm
	// TODO 日志相关
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ { // 发送heartbeat
		if i == rf.me {
			continue
		} else if rf.killed() {
			return
		}
		go func(peer int) {
			rf.mu.Lock()
			if rf.role != leader {
				rf.mu.Unlock()
				return
			}

			// TODO 日志相关
			args := &AppendEntriesArgs{
				Term:     term,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()

			reply := new(AppendEntriesReply)
			rf.sendAppendEntries(peer, args, reply)
			DPrintf("[term %d] [server %d] receive a AppendEntries RPC response with term %d from peer %d", rf.currentTerm, rf.me, reply.Term, peer)

			rf.mu.Lock()
			if rf.role != leader || rf.currentTerm != args.Term { // 过期响应或不再是leader
				rf.mu.Unlock()
				return
			}

			if reply.Term > rf.currentTerm { // 发现更新的, 转变为follower
				rf.mu.Unlock()
				rf.convertToFollower(reply.Term, args.LeaderId)
			}
		}(i)
	}
}

// heartbeat 循环发送AppendEntries
func (rf *Raft) heartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		DPrintf("[term %d] [server %d] will send a round of AppendEntries RPC", rf.currentTerm, rf.me)
		rf.appendEntries()
	}
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// convertToFollower 转变为follower
func (rf *Raft) convertToFollower(term, leaderId int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch rf.role {
	case follower:
	case candidate:
		DPrintf("[term %d] [server %d] become a follower from role: %s", rf.currentTerm, rf.me, rf.role)
	case leader:
		DPrintf("[term %d] [server %d] become a follower from role: %s", rf.currentTerm, rf.me, rf.role)
	}

	rf.currentTerm = term
	rf.reTimer(randDur())
	rf.votedFor = -1
	rf.leaderId = leaderId
	rf.role = follower
}

// 转变为candidate
func (rf *Raft) convertToCandidate() {
	// 更新信息
	rf.mu.Lock()
	switch rf.role {
	case follower: // 初次选举
		DPrintf("[term %d] [server %d] become a candidate", rf.currentTerm, rf.me)
	case candidate: // 重新选举
		DPrintf("[term %d] [server %d] request for vote again", rf.currentTerm, rf.me)
	}
	rf.role = candidate
	rf.currentTerm, rf.votedFor = rf.currentTerm+1, rf.me // 更新term并投票给自己
	term := rf.currentTerm                                // peer总数与当前任期
	rf.reTimer(randDur())                                 // 重新计时
	rf.mu.Unlock()

	var (
		votes, finish, all = 1, 1, len(rf.peers)
		mu                 sync.Mutex
		cond               = sync.NewCond(&mu)
	)

	// 发起投票
	for i := 0; i < all; i++ {
		if i == rf.me { // 跳过自己
			continue
		} else if rf.killed() { // 判断是否被kill
			return
		}

		rf.mu.Lock()
		if rf.role != candidate { // 不为candidate则退出
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		go func(peer int) {
			rf.mu.Lock()
			args := &RequestVoteArgs{
				Term:        term,
				CandidateId: rf.me,
				//LastLogIndex: ,
				//LastLogTerm:  ,
			}
			rf.mu.Unlock()
			reply := new(RequestVoteReply)
			rf.sendRequestVote(peer, args, reply)

			rf.mu.Lock()
			if reply.Term > rf.currentTerm { // 发现更新的term, 从candidate退为follower
				rf.mu.Unlock()
				rf.convertToFollower(reply.Term, -1)
				return
			}
			rf.mu.Unlock()

			mu.Lock()
			finish++
			if reply.VoteGranted { // 获得投票
				DPrintf("[term %d] [server %d] won a vote from peer %d", rf.currentTerm, rf.me, peer)
				votes++
			}
			cond.Broadcast()
			mu.Unlock()
		}(i)

	}

	mu.Lock()
	defer mu.Unlock()
	for votes < all/2+1 && finish < all {
		cond.Wait()
		DPrintf("[term %d] [server %d] has send %d voteRequest and receive %d votes of total %d", rf.currentTerm, rf.me, finish, votes, all)

		rf.mu.Lock()
		stillCandidate := rf.role == candidate
		rf.mu.Unlock()
		if !stillCandidate {
			return
		}
	}

	if votes >= all/2+1 {
		rf.convertToLeader()
	}
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[term %d] [server %d] become a leader", rf.currentTerm, rf.me)
	rf.role = leader
	rf.votedFor = -1
	rf.leaderId = rf.me

	// TODO log 相关
	rf.reTimer(0) // 立即开始heartbeat
	return
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[term %d] [server %d] was killed", rf.currentTerm, rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for {
		select {
		case <-rf.timer.C: // 计时器到时间了
			if rf.killed() { // 节点关闭则退出
				return
			}
			rf.mu.Lock()
			switch rf.role {
			case follower: // follower选举超时, 转变为candidate
				go rf.convertToCandidate()
			case candidate: // candidate选举超时, 重新投票
				go rf.convertToCandidate()
			case leader:
				go rf.heartbeat()
				rf.timer.Reset(heartbeatGap)
			}
			rf.mu.Unlock()
		}
	}
}

// randDur 随机时间
func randDur() time.Duration {
	return time.Duration(50+(rand.Int63()%300)) * time.Millisecond
}

func (rf *Raft) reTimer(duration time.Duration) {
	rf.timer.Stop()
	rf.timer.Reset(duration)
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		currentTerm: 0,
		votedFor:    -1,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   nil,
		matchIndex:  nil,
		role:        follower,
		idle:        true,
		timer:       time.NewTimer(randDur()),
	}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
