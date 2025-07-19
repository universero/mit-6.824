package rsm

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
	"fmt"
	"sync"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	Id   int           // 唯一标识
	Me   int           // rsm id
	Term int           // 请求时term
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
	fmt.Printf("[rsm %d] [term %d] submit op %+v\n", rsm.me, term, op)
	rsm.waits[op.Id] = op
	rsm.mu.Unlock()

	<-op.Done // when the op was finished
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	fmt.Printf("[rsm %d] [submit] weak up %d %+v with err %s\n", rsm.me, index, op, op.Err)
	if term, isLeader = rsm.rf.GetState(); term > op.Term || !isLeader || op.Err != rpc.OK {
		if _, ok := rsm.waits[op.Id]; ok {
			delete(rsm.waits, op.Id)
			var toDelete []int
			for id, pend := range rsm.waits {
				if pend.Term < op.Term || (pend.Term == op.Term && pend.Id > op.Id) {
					toDelete = append(toDelete, id)
				}
			}
			// 再处理
			for _, id := range toDelete {
				fmt.Printf("close %d\n", id)
				if id != op.Id {
					close(rsm.waits[id].Done)
				}
			}
		}
		return rpc.ErrWrongLeader, nil
	}
	delete(rsm.waits, op.Id)
	return rpc.OK, op.Resp
}

func (rsm *RSM) reader() {
	var term int
	//var isLeader bool
	for {
		select {
		case msg, _ := <-rsm.applyCh: // receive a finish op
			rsm.mu.Lock()
			if op, exist := rsm.waits[msg.CommandIndex]; exist {
				op.Err, op.Resp = rpc.ErrWrongLeader, nil
				if term, _ = rsm.rf.GetState(); term == op.Term && op.Req == msg.Command {
					op.Err = rpc.OK
					if msg.Command != nil {
						op.Resp = rsm.sm.DoOp(msg.Command) // 执行操作
					}
				}
				close(op.Done)
			} else if msg.Command != nil {
				rsm.sm.DoOp(msg.Command)
			}
			rsm.mu.Unlock()
		}
	}
}
