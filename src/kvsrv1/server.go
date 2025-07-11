package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// tuple 表示 key, value, version 三元组
type tuple struct {
	key     string
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu  sync.Mutex
	kvs map[string]*tuple
}

func MakeKVServer() *KVServer {
	kv := &KVServer{kvs: make(map[string]*tuple)}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	reply.Err = rpc.ErrNoKey
	if kvv, ok := kv.kvs[args.Key]; ok {
		reply.Err = rpc.OK
		reply.Value = kvv.value
		reply.Version = kvv.version
	}
	return
}

// Put Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Err = rpc.OK
	if kvv, ok := kv.kvs[args.Key]; ok { // key存在
		if kvv.version == args.Version { // 版本匹配
			kvv.value = args.Value
			kvv.version++
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

// Kill You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// StartKVServer You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
