package rsm

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
	"sync"
	"time"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	Id   int           // 唯一标识
	Me   int           // rsm id
	Term int           // term
	Req  any           // 请求
	Resp any           // 响应
	Err  rpc.Err       // 错误
	Done chan struct{} // 完成
}

// StateMachine A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	waits     map[int]*Op // Submit等待
	termWaits map[int][]*Op
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waits:        make(map[int]*Op),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// submit op and wait for weak up
	rsm.mu.Lock()
	index, term, isLeader := rsm.rf.Start(req) // try to submit the op
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	op := &Op{Id: index, Me: rsm.me, Term: term, Req: req, Done: make(chan struct{})}
	//fmt.Printf("[rsm %d] [term %d] submit op %+v\n", rsm.me, term, op)
	rsm.waits[op.Id] = op
	rsm.mu.Unlock()
	for {
		select {
		case <-op.Done:
			if term != op.Term {
				return rpc.ErrWrongLeader, nil
			}
			return op.Err, op.Resp
		case <-time.After(100 * time.Millisecond):
			rsm.mu.Lock()
			if currentTerm, stillLeader := rsm.rf.GetState(); currentTerm == term && stillLeader {
				rsm.mu.Unlock()
				continue
			}
			delete(rsm.waits, op.Id)
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		case <-time.After(3 * time.Second):
			rsm.mu.Lock()
			delete(rsm.waits, op.Id)
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
	}
}

func (rsm *RSM) reader() {
	var term int
	var resp any
	for {
		select {
		case msg, ok := <-rsm.applyCh: // receive a finish op
			if !ok {
				return
			}
			rsm.mu.Lock()
			if msg.Command != nil {
				//fmt.Printf("[rsm %d] doing %+v \n", rsm.me, msg.Command)
				resp = rsm.sm.DoOp(msg.Command) // 执行操作
				if op, exist := rsm.waits[msg.CommandIndex]; exist {
					term, _ = rsm.rf.GetState()
					op.Err, op.Resp = rpc.OK, resp
					//fmt.Printf("[rsm %d] [term %d] submit msg %+v with error %s \n", rsm.me, term, msg, op.Err)
					op.Term = term
					close(op.Done)
					delete(rsm.waits, msg.CommandIndex)
				}
			}
			rsm.mu.Unlock()
		}
	}
}
