package kvraft

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"sync"
	"sync/atomic"
	"time"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	mu      sync.Mutex
	recent  atomic.Int32 // 最近一次的leader
	n       int32
	seq     atomic.Int32
	me      string
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, n: int32(len(servers)), me: kvtest.RandValue(len(servers))}
	ck.recent.Store(0)
	ck.seq.Store(0)
	return ck
}

// Get fetches the current Value and Version for a Key.  It returns
// ErrNoKey if the Key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args, reply := &rpc.GetArgs{Key: key}, rpc.GetReply{}
	// 重复直到成功
	for !ck.clnt.Call(ck.servers[ck.recent.Load()%ck.n], "KVServer.Get", args, &reply) || reply.Err == rpc.ErrWrongLeader {
		reply = rpc.GetReply{}
		ck.recent.Add(1)
		time.Sleep(100 * time.Millisecond)
	}
	//fmt.Printf("[get] |Key: %s|Value: %s|gversion: %d|err: %s\n", args.Key, reply.Value, reply.Version, reply.Err)
	//fmt.Printf("[Clerk] Get Reply: %+v\n", reply)
	return reply.Value, reply.Version, reply.Err
}

// Put updates Key with Value only if the Version in the
// request matches the Version of the Key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	retries, seq := 0, ck.seq.Add(1)
	args, reply := &rpc.PutArgs{Key: key, Value: value, Version: version, Seq: seq, CkId: ck.me}, rpc.PutReply{}
	//fmt.Printf("[Clerk %s] Put Args: Key %s Value %s Version %v seq %d\n", ck.me, Key, Value, Version, seq)
	for {
		for !ck.clnt.Call(ck.servers[ck.recent.Load()%ck.n], "KVServer.Put", args, &reply) || reply.Err == rpc.ErrWrongLeader {
			if reply.Err != rpc.ErrWrongLeader {
				retries++
			}
			reply = rpc.PutReply{}
			ck.recent.Add(1)
			time.Sleep(100 * time.Millisecond)
		}
		if retries > 0 && reply.Err == rpc.ErrVersion {
			//fmt.Printf("[Clerk %s] maybe: %+v\n", ck.me, reply)
			return rpc.ErrMaybe
		} else if retries > 0 {
			//fmt.Printf("[Clerk %s] retry but nomaybe: %+v\n", ck.me, reply)
		}
		//fmt.Printf("[Clerk %s] Put Reply: %+v wit retry %d\n", ck.me, reply, retries)
		return reply.Err
	}
}
