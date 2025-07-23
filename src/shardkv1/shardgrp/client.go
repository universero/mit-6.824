package shardgrp

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
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
	args, reply := &rpc.GetArgs{Shid: shardcfg.Key2Shard(key), Key: key}, rpc.GetReply{}
	// 重复直到成功
	for !ck.clnt.Call(ck.servers[ck.recent.Load()%ck.n], "KVServer.Get", args, &reply) || reply.Err == rpc.ErrWrongLeader {
		reply = rpc.GetReply{}
		ck.recent.Add(1)
		time.Sleep(100 * time.Millisecond)
	}
	return reply.Value, reply.Version, reply.Err
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	retries, seq := 0, ck.seq.Add(1)
	args, reply := &rpc.PutArgs{Shid: shardcfg.Key2Shard(key), Key: key, Value: value, Version: version, Seq: seq, CkId: ck.me}, rpc.PutReply{}

	for !ck.clnt.Call(ck.servers[ck.recent.Load()%ck.n], "KVServer.Put", args, &reply) || reply.Err == rpc.ErrWrongLeader {
		if reply.Err != rpc.ErrWrongLeader {
			retries++
		}
		reply = rpc.PutReply{}
		ck.recent.Add(1)
		time.Sleep(100 * time.Millisecond)
	}
	if retries > 0 && reply.Err == rpc.ErrVersion {
		return rpc.ErrMaybe
	}
	return reply.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := &shardrpc.FreezeShardArgs{Shard: s, Num: num}
	reply := shardrpc.FreezeShardReply{}
	for !ck.clnt.Call(ck.servers[ck.recent.Load()%ck.n], "KVServer.FreezeShard", args, &reply) || reply.Err == rpc.ErrWrongLeader {
		reply = shardrpc.FreezeShardReply{}
		ck.recent.Add(1)
		time.Sleep(100 * time.Millisecond)
	}
	DPrintf("[client] freeze shard %d with response %s\n", s, reply.Err)
	return reply.State, reply.Err
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := &shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	reply := shardrpc.InstallShardReply{}
	for !ck.clnt.Call(ck.servers[ck.recent.Load()%ck.n], "KVServer.InstallShard", args, &reply) || reply.Err == rpc.ErrWrongLeader {
		reply = shardrpc.InstallShardReply{}
		ck.recent.Add(1)
		time.Sleep(100 * time.Millisecond)
	}
	return reply.Err
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := &shardrpc.DeleteShardArgs{Shard: s, Num: num}
	reply := shardrpc.DeleteShardReply{}
	for !ck.clnt.Call(ck.servers[ck.recent.Load()%ck.n], "KVServer.DeleteShard", args, &reply) || reply.Err == rpc.ErrWrongLeader {
		reply = shardrpc.DeleteShardReply{}
		ck.recent.Add(1)
		time.Sleep(100 * time.Millisecond)
	}
	return reply.Err
}
