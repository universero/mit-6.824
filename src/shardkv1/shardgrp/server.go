package shardgrp

import (
	"6.5840/shardkv1/shardcfg"
	"bytes"
	"io"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	mu           sync.Mutex
	kvs          map[string]*tuple
	smu          sync.Mutex
	ckSeq        map[string]*rpc.PutReply
	frozenShards map[shardcfg.Tshid]bool // record the frozen shards
	num          [3]shardcfg.Tnum
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
	case shardrpc.FreezeShardArgs:
		if refine, ok := req.(shardrpc.FreezeShardArgs); ok {
			return kv.doFreezeShard(&refine)
		}
	case *shardrpc.FreezeShardArgs:
		if refine, ok := req.(*shardrpc.FreezeShardArgs); ok {
			return kv.doFreezeShard(refine)
		}
	case shardrpc.InstallShardArgs:
		if refine, ok := req.(shardrpc.InstallShardArgs); ok {
			return kv.doInstallShard(&refine)
		}
	case *shardrpc.InstallShardArgs:
		if refine, ok := req.(*shardrpc.InstallShardArgs); ok {
			return kv.doInstallShard(refine)
		}
	case shardrpc.DeleteShardArgs:
		if refine, ok := req.(shardrpc.DeleteShardArgs); ok {
			return kv.doDeleteShard(&refine)
		}
	case *shardrpc.DeleteShardArgs:
		if refine, ok := req.(*shardrpc.DeleteShardArgs); ok {
			return kv.doDeleteShard(refine)
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
}

func (kv *KVServer) doGet(args *rpc.GetArgs) (reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.frozenShards[args.Shid] { // check whether the key is frozen
		reply.Err = rpc.ErrWrongGroup
		return
	}

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
}

func (kv *KVServer) doPut(args *rpc.PutArgs) (reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.frozenShards[args.Shid] { // check whether the key is frozen
		reply.Err = rpc.ErrWrongGroup
		return
	}

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

// FreezeShard Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	var resp any
	err, resp := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = *(resp.(*shardrpc.FreezeShardReply))
}
func (kv *KVServer) doFreezeShard(args *shardrpc.FreezeShardArgs) (reply *shardrpc.FreezeShardReply) {
	kv.mu.Lock()
	reply = &shardrpc.FreezeShardReply{Num: kv.num[0], Err: "outdated configuration"}
	if args.Num < kv.num[0] { // outdated config
		kv.mu.Unlock()
		return
	}

	kv.frozenShards[args.Shard] = true // froze the shard

	state := make(map[string]tuple)
	for k, v := range kv.kvs {
		if shardcfg.Key2Shard(k) == args.Shard {
			state[k] = tuple{Key: v.Key, Value: v.Value, Version: v.Version}
		}
	}
	kv.mu.Unlock()

	w := new(bytes.Buffer)
	_ = labgob.NewEncoder(w).Encode(state)
	kv.num[0] = args.Num
	reply.Err, reply.State, reply.Num = rpc.OK, w.Bytes(), kv.num[0]
	return
}

// InstallShard Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	var resp any
	err, resp := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = *(resp.(*shardrpc.InstallShardReply))
}

func (kv *KVServer) doInstallShard(args *shardrpc.InstallShardArgs) (reply *shardrpc.InstallShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply = &shardrpc.InstallShardReply{Err: "outdated configuration"}
	if args.Num < kv.num[1] {
		return
	}

	state := make(map[string]tuple)
	d := labgob.NewDecoder(bytes.NewBuffer(args.State))
	if err := d.Decode(&state); err != io.EOF && err != nil {
		reply.Err = "decode error"
		return
	}
	for k, v := range state {
		kv.kvs[k] = &v
	}
	if state != nil && len(state) > 0 {
		//fmt.Printf("[server %d] state %+v\n kvs %+v\n", kv.me, state, kv.kvs)
	}
	kv.num[1] = args.Num
	reply.Err = rpc.OK
	return
}

// DeleteShard Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	var resp any
	err, resp := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = *(resp.(*shardrpc.DeleteShardReply))
}
func (kv *KVServer) doDeleteShard(args *shardrpc.DeleteShardArgs) (reply *shardrpc.DeleteShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply = &shardrpc.DeleteShardReply{Err: "outdated configuration"}
	if args.Num < kv.num[2] {
		return
	}
	var toDelete []string
	for k := range kv.kvs {
		if shardcfg.Key2Shard(k) == args.Shard {
			toDelete = append(toDelete, k)
		}
	}
	for _, k := range toDelete {
		delete(kv.kvs, k)
	}
	delete(kv.frozenShards, args.Shard)
	kv.num[2] = args.Num
	reply.Err = rpc.OK
	return
}

// Kill the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartServerShardGrp StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.kvs = make(map[string]*tuple)
	kv.ckSeq = make(map[string]*rpc.PutReply)
	kv.frozenShards = make(map[shardcfg.Tshid]bool)
	kv.num = [3]shardcfg.Tnum{0, 0, 0}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []tester.IService{kv, kv.rsm.Raft()}
}
