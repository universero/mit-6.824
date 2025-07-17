package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"fmt"

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

var gap int64 = 110

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int     // 当前任期号(初始0), 持久化
	votedFor    int     // 已投票对象(初始-1), 持久化
	log         []Entry // 日志条目, 持久化

	commitIndex int // 已提交的最大日志索引, 易失
	lastApplied int // 已应用的最大日志索引, 易失

	nextIndex         []int  // 每个server对应的下一个日志条目索引, leader
	matchIndex        []int  // 每个server对应的已复制的最大索引, leader
	leaderId          int    // 当前leader
	role              string // follower, candidate, leader
	electionInterval  int64  // 选举时间间隔
	heartbeatInterval int64  // 心跳间隔
	heartbeatTime     int64  // 上一次收到心跳消息时间
	applyCh           chan raftapi.ApplyMsg
	cond              *sync.Cond
	peerCond          []*sync.Cond
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()
	return rf.currentTerm, rf.role == leader
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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()
	term, index := rf.currentTerm, rf.log[len(rf.log)-1].Index+1
	if rf.role != leader { // 不是leader
		return index, term, false
	}

	entry := Entry{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.peerCond[peer].Signal()
	}

	return index, term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("[term %d] [server %d] was killed with status\n%s\n", rf.currentTerm, rf.me, rf.String())
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.votedFor, rf.currentTerm, rf.role, rf.leaderId = -1, 0, follower, -1
	rf.log = []Entry{{Index: 0, Term: 0, Command: nil}}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh, rf.heartbeatTime, rf.heartbeatInterval = applyCh, time.Now().UnixMilli(), gap
	rf.ResetElectionTimeout()
	rf.cond = sync.NewCond(&rf.mu)
	rf.peerCond = make([]*sync.Cond, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.peerCond[i] = sync.NewCond(&sync.Mutex{})
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.StartElect()         // 开始选举
	go rf.SendHeartbeat()      // 发送心跳
	go rf.UpdateStateMachine() // 更新日志
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.ReplicateLog(peer) // 每个peer复制日志
	}
	return rf
}

func (rf *Raft) String() string {
	var builder strings.Builder
	// 基础信息（节点ID、角色、LeaderID）
	builder.WriteString(fmt.Sprintf("[Raft %d | %s | LeaderId %d]\n",
		rf.me,
		strings.ToUpper(rf.role[:1])+rf.role[1:], rf.leaderId))
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
	}
	return builder.String()
}
