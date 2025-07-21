package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	mu    sync.Mutex
	kvs   map[string]*tuple
	smu   sync.Mutex
	ckSeq map[string]*rpc.PutReply
}

type tuple struct {
	Key     string
	Value   string
	Version rpc.Tversion
}

func (kv *KVServer) DoOp(req any) any {
	switch req.(type) {
	case rpc.GetArgs:
		if refine, ok := req.(rpc.GetArgs); ok {
			return kv.doGet(&refine)
		}
	case *rpc.GetArgs:
		if refine, ok := req.(*rpc.GetArgs); ok {
			return kv.doGet(refine)
		}
	case rpc.PutArgs:
		if refine, ok := req.(rpc.PutArgs); ok {
			return kv.doPut(&refine)
		}
	case *rpc.PutArgs:
		if refine, ok := req.(*rpc.PutArgs); ok {
			return kv.doPut(refine)
		}
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(kv.kvs)
	_ = e.Encode(kv.ckSeq)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	d := labgob.NewDecoder(bytes.NewBuffer(data))
	var kvs map[string]*tuple
	var ckSeq map[string]*rpc.PutReply
	if err := d.Decode(&kvs); err != nil {
		panic(err)
	}
	if err := d.Decode(&ckSeq); err != nil {
		panic(err)
	}
	kv.kvs, kv.ckSeq = kvs, ckSeq
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	var resp any
	err, resp := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = *(resp.(*rpc.GetReply))
	//fmt.Printf("[kvserver %d] Get Reply: %+v\n", kv.me, reply)
}

func (kv *KVServer) doGet(args *rpc.GetArgs) (reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//fmt.Printf("[kvserver %d] Get Args: %+v\n", kv.me, args)
	reply = &rpc.GetReply{Err: rpc.ErrNoKey}
	if kvv, ok := kv.kvs[args.Key]; ok {
		reply.Err = rpc.OK
		reply.Value = kvv.Value
		reply.Version = kvv.Version
	}
	return
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	if last, ok := kv.ckSeq[args.CkId]; ok && last.Seq >= args.Seq { // 做过的请求
		reply.Err, reply.Seq = last.Err, last.Seq
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var resp any
	err, resp := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = *(resp.(*rpc.PutReply))
	//fmt.Printf("[kvserver %d] Put Reply: %+v\n", kv.me, reply)
}

func (kv *KVServer) doPut(args *rpc.PutArgs) (reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//fmt.Printf("[kvserver %d] Put Args: %+v\n", kv.me, args)
	if last, ok := kv.ckSeq[args.CkId]; ok && last.Seq >= args.Seq { // 做过的请求
		reply = &rpc.PutReply{Err: last.Err, Seq: last.Seq}
		return
	}
	defer func() { kv.ckSeq[args.CkId] = reply }()
	reply = &rpc.PutReply{Err: rpc.OK, Seq: args.Seq}
	if kvv, ok := kv.kvs[args.Key]; ok { // key存在
		if kvv.Version == args.Version { // 版本匹配
			kvv.Value = args.Value
			kvv.Version++
		} else { // 版本不匹配
			reply.Err = rpc.ErrVersion
		}
		return
	}

	if args.Version == 0 { // 不存在且版本为0, 新增kv
		kv.kvs[args.Key] = &tuple{args.Key, args.Value, 1}
	} else { // 不存在且版本不为0
		reply.Err = rpc.ErrNoKey
	}
	return
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
	//fmt.Printf("[kvserver %d] %++v\n", kv.me, kv.kvs)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	kv := &KVServer{me: me}

	kv.kvs = make(map[string]*tuple)
	kv.ckSeq = make(map[string]*rpc.PutReply)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []tester.IService{kv, kv.rsm.Raft()}
}
