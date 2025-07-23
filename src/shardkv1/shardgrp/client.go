package shardgrp

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
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

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers, n: int32(len(servers)), me: kvtest.RandValue(len(servers))}
	ck.recent.Store(0)
	ck.seq.Store(0)
	return ck
}

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

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}
