package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"fmt"
	//	"bytes"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

const (
	follower  = "follower"
	candidate = "candidate"
	leader    = "leader"
)

var heartbeatGap = time.Duration(200) * time.Millisecond

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex            // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd   // RPC end points of all peers
	persister   *tester.Persister     // Object to hold this peer's persisted state
	me          int                   // this peer's index into peers[]
	dead        int32                 // set by Kill()
	currentTerm int                   // 当前任期号(初始0), 持久化
	votedFor    int                   // 已投票对象(初始-1), 持久化
	log         []Log                 // 日志条目, 持久化
	commitIndex int                   // 已提交的最大日志索引, 易失
	lastApplied int                   // 已应用的最大日志索引, 易失
	nextIndex   []int                 // 每个server对应的下一个日志条目索引, leader
	matchIndex  []int                 // 每个server对应的已复制的最大索引, leader
	role        string                // 0 follower, 1 candidate, 2 leader
	idle        bool                  // 是否收到过leader的RPC或candidate的GrantVote
	leaderId    int                   // 此时leaderId
	electing    bool                  // 是否在选举中
	applyCh     chan raftapi.ApplyMsg // 应用的日志
}

type Log struct {
	Term    int         // 日志对应任期
	Index   int         // 日志对应索引
	Command interface{} // 命令
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm // 当前任期
		return
	}
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.leaderId = -1
		rf.role = follower
		rf.currentTerm = args.Term // 更新任期
	}

	reply.Term = rf.currentTerm

	latest := rf.log[len(rf.log)-1]
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	} else if (latest.Term > args.LastLogTerm) || (latest.Term == args.LastLogTerm && latest.Index > args.LastLogIndex) {
		reply.VoteGranted = false
		DPrintf("[term %d] [server %d] denied to vote to %d because of outdate log: lastest: %+v ; args: %+v", rf.currentTerm, rf.me, args.CandidateId, latest, args)
		return
	}
	DPrintf("[term %d] [server %d] vote for peer %d", rf.currentTerm, rf.me, args.CandidateId)
	reply.VoteGranted = true // 确认投票
	rf.leaderId = -1
	rf.votedFor = args.CandidateId // 记录投票对象
	rf.idle = false                // 接受过GrantVote的RPC
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
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[term %d] [server %d] receive a AppendEntries RPC Request with term %d \n%+v\n", rf.currentTerm, rf.me, args.Term, args)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// 新任期或者当前任期有了leader, 转换为follower
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		go rf.convertToFollower()
	}
	rf.idle = false
	rf.leaderId = args.LeaderId

	prev := args.PrevLogIndex
	last := rf.log[len(rf.log)-1]
	if last.Index < prev || rf.log[prev].Term != args.PrevLogTerm { // prevLog存在冲突
		reply.Success = false
		reply.Term = rf.currentTerm
		// 找到冲突任期的第一条日志
		prev = min(prev, len(rf.log)-1)
		reply.ConflictTerm = rf.log[prev].Term
		for i := prev; i >= 1; i-- {
			if rf.log[i].Term != rf.log[i-1].Term {
				reply.ConflictIndex = rf.log[i].Index
				break
			}
		}
		DPrintf("[term %d] [server %d] try to append entries but found conflict at index %d term %d", rf.currentTerm, rf.me, reply.ConflictIndex, reply.ConflictTerm)
		return
	}

	var i int
	for _, entry := range args.Entries {
		i++
		if prev+i >= len(rf.log) || entry.Term != rf.log[prev+i].Term { // 不具有或者出现不一致
			rf.log = rf.log[:min(prev+i, len(rf.log))] // 删除后面不一致的
			DPrintf("[term %d] [server %d] delete outdate log %+v and append new log \n%+v\n", rf.currentTerm, rf.me, rf.log[min(prev+i, len(rf.log)):], args.Entries[i-1:])
			rf.log = append(rf.log, args.Entries[i-1:]...) // 添加新的
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		DPrintf("[term %d] [server %d] update commitIndex to %d", rf.currentTerm, rf.me, rf.commitIndex)
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	return
}

// 发送AppendEntries请求, 闲时作为心跳请求WW
func (rf *Raft) appendEntries() {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index // 更新leader自己的matchIndex，就等于其log最后一个日志条目的index
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1     // 更新leader自己的nextIndex
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ { // 发送heartbeat
		if i == rf.me {
			continue
		}
		go func(peer int) {
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			if rf.role != leader {
				rf.mu.Unlock()
				return
			}

			var entries []Log               // 需要追加的日志
			nex := rf.nextIndex[peer]       // 下一个日志的索引号
			latest := rf.log[len(rf.log)-1] // 最新的日志
			if latest.Index >= nex {        // 最新的日志大于该peer最新的, 需要传递的新日志
				entries = make([]Log, len(rf.log)-nex)
				copy(entries, rf.log[nex:])
			}

			preLog := rf.log[nex-1] // 新条目前一个条目
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: preLog.Index,
				PrevLogTerm:  preLog.Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex, // leader已提交的最新日志索引
			}
			rf.mu.Unlock()
			reply := new(AppendEntriesReply)
			if !rf.sendAppendEntries(peer, args, reply) {
				DPrintf("[term %d] [server %d] meets a network failure when send AppendEntries RPC to peer %d\n", rf.currentTerm, rf.me, peer)
				return
			}
			DPrintf("[term %d] [server %d] receive a AppendEntries RPC response with term %d from peer %d\n%+v\n", rf.currentTerm, rf.me, reply.Term, peer, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != leader || rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm { // 发现更新的, 转变为follower
				rf.currentTerm = reply.Term
				go rf.convertToFollower()
				return
			}

			if !reply.Success { // follower和leader日志不一致,需要减少nextIndex并重试
				rf.nextIndex[peer] = max(1, reply.ConflictIndex) // 将nextIndex重置为冲突任期的第一个, 等待下一次重试, 但如果为0说明follower的日志是空的, 这时候应该从一开始
			} else { // 追加成功
				if len(args.Entries) != 0 {
					rf.matchIndex[peer] = max(rf.matchIndex[peer], args.PrevLogIndex+len(args.Entries))
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1

					// 更新commitIndex
					sortMatchIndex := make([]int, len(rf.peers))
					copy(sortMatchIndex, rf.matchIndex)
					sort.Ints(sortMatchIndex)
					maxN := sortMatchIndex[(len(sortMatchIndex)-1)/2]
					for N := maxN; N > rf.commitIndex; N-- {
						if rf.log[N].Term == rf.currentTerm {
							DPrintf("[term %d] [server %d] update its commitIndex from %d to %d", rf.currentTerm, rf.me, rf.commitIndex, N)
							rf.commitIndex = N
							break
						}
					}
				}
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
		//DPrintf("[term %d] [server %d] will send a round of AppendEntries RPC", rf.currentTerm, rf.me)
		go rf.appendEntries()
		time.Sleep(heartbeatGap)
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
func (rf *Raft) convertToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != follower {
		DPrintf("[term %d] [server %d] become a follower from role: %s", rf.currentTerm, rf.me, rf.role)
	}
	// leader是没有ticker循环的, 从leader变成follower需要打开计时;
	// 但是candidate是有循环的, 不需要启动计时
	if rf.role == leader {
		go rf.ticker()
	}
	rf.votedFor = -1
	rf.role = follower
	rf.idle = true
	rf.electing = false
	return
}

// 转变为candidate
func (rf *Raft) convertToCandidate() {
	// 更新信息
	rf.mu.Lock()
	rf.currentTerm, rf.votedFor = rf.currentTerm+1, rf.me // 更新term并投票给自己
	if rf.role == follower {
		DPrintf("[term %d] [server %d] become a candidate", rf.currentTerm, rf.me)
	} else {
		DPrintf("[term %d] [server %d] request for vote again", rf.currentTerm, rf.me)
	}
	rf.role, rf.electing = candidate, true
	all, term := len(rf.peers), rf.currentTerm // peer总数与当前任期
	rf.mu.Unlock()

	var (
		votes, finish = 1, 1
		mu            sync.Mutex
		cond          = sync.NewCond(&mu)
	)

	// 发起投票
	for i := 0; i < all; i++ {
		if i == rf.me {
			continue
		}

		if rf.killed() { // 判断是否被kill
			return
		}
		rf.mu.Lock()
		if rf.role != candidate { // 判断是否仍为candidate
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		go func(peer int) {
			rf.mu.Lock()
			latest := rf.log[len(rf.log)-1]
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: latest.Index,
				LastLogTerm:  latest.Term,
			}
			rf.mu.Unlock()
			reply := new(RequestVoteReply)
			rf.sendRequestVote(peer, args, reply)

			rf.mu.Lock()
			if reply.Term > rf.currentTerm { // 发现更新的term
				rf.currentTerm = reply.Term
				rf.mu.Unlock()
				go rf.convertToFollower()
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
	for votes < all/2+1 && finish < all {
		cond.Wait()
		DPrintf("[term %d] [server %d] has send %d voteRequest and receive %d votes of total %d", rf.currentTerm, rf.me, finish, votes, all)

		rf.mu.Lock()
		stillCandidate := rf.role == candidate
		rf.mu.Unlock()
		if !stillCandidate {
			mu.Unlock()
			return
		}
	}

	if votes >= all/2+1 {
		go rf.convertToLeader()
	}
	mu.Unlock()
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[term %d] [server %d] become a leader", rf.currentTerm, rf.me)
	rf.role = leader
	rf.votedFor = -1
	rf.leaderId = rf.me

	n := len(rf.log)
	for peer := range len(rf.peers) {
		rf.nextIndex[peer] = n
	}
	go rf.heartbeat()
	return
}

// Start the service using Raft (e.g. a k/v server) wants to start
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	index = len(rf.log)
	// 不是leader
	if rf.role != leader {
		return term, index, false
	}

	entries := Log{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.log = append(rf.log, entries)
	go rf.appendEntries()
	return index, term, true
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[term %d] [server %d] was killed with status \n%s\n", rf.currentTerm, rf.me, rf.String())
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		switch rf.role {
		case follower:
			if !rf.idle {
				//DPrintf("[term %d] [server %d] was not idle as a follower", rf.currentTerm, rf.me)
				rf.idle = true // 重置
				rf.mu.Unlock()
				break
			}
			DPrintf("[term %d] [server %d] was idle as a follower", rf.currentTerm, rf.me)
			// 空闲了一段时间, 应该转变为candidate
			rf.electing = false
			fallthrough
		case candidate:
			if !rf.electing {
				go rf.convertToCandidate() // 开始选举
			} else {
				rf.electing = false
			}
			rf.mu.Unlock()
		case leader: // leader 不需要选举超时
			rf.mu.Unlock()
			return
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		var need []Log
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			need = append(need, rf.log[rf.lastApplied+1:rf.commitIndex+1]...)
		}
		rf.lastApplied = rf.commitIndex
		if len(need) > 0 {
			for _, log := range need {
				msg := raftapi.ApplyMsg{
					CommandValid: true,
					Command:      log.Command,
					CommandIndex: log.Index,
				}
				rf.applyCh <- msg
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		mu:          sync.Mutex{},
		peers:       peers,
		persister:   persister,
		me:          me,
		dead:        0,
		currentTerm: 0,
		votedFor:    -1,
		log: []Log{{ // 所有的peer初始化时都会有一个0日志, 且一定一致
			Term:    0,
			Index:   0,
			Command: nil,
		}},
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		role:        follower,
		idle:        true,
		leaderId:    -1,
		electing:    false,
		applyCh:     applyCh,
	}

	// Your initialization code here (3A, 3B, 3C).
	go rf.applier()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) String() string {
	var builder strings.Builder

	// 基础信息（节点ID、角色、LeaderID）
	builder.WriteString(fmt.Sprintf("[Raft %d | %s | LeaderID: %d]\n",
		rf.me,
		strings.ToUpper(rf.role[:1])+rf.role[1:], // 首字母大写
		rf.leaderId))

	// 任期和投票信息
	builder.WriteString(fmt.Sprintf("├─ CurrentTerm: %d | VotedFor: %d\n",
		rf.currentTerm,
		rf.votedFor))

	// 完整日志输出
	builder.WriteString("├─ Log Entries:\n")
	if len(rf.log) == 0 {
		builder.WriteString("│   <empty>\n")
	} else {
		for i, entry := range rf.log {
			builder.WriteString(fmt.Sprintf("│   %4d: {Term: %d, Cmd: %v}\n",
				i,
				entry.Term,
				entry.Command))
		}
	}

	// 提交和应用进度
	builder.WriteString(fmt.Sprintf("├─ CommitIndex: %d | LastApplied: %d\n",
		rf.commitIndex,
		rf.lastApplied))

	// Leader专属信息
	if rf.role == "leader" {
		builder.WriteString(fmt.Sprintf("├─ NextIndex: %v\n", rf.nextIndex))
		builder.WriteString(fmt.Sprintf("└─ MatchIndex: %v", rf.matchIndex))
	} else {
		builder.WriteString(fmt.Sprintf("└─ Idle: %v | Electing: %v",
			rf.idle,
			rf.electing))
	}

	return builder.String()
}
