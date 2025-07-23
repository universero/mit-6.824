package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
	"time"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
}

// MakeClerk The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (value string, version rpc.Tversion, err rpc.Err) {
	var ok bool
	var servers []string
	var shid shardcfg.Tshid
	var cfg *shardcfg.ShardConfig

	for {
		cfg = ck.sck.Query()
		shid = shardcfg.Key2Shard(key)
		if _, servers, ok = cfg.GidServers(shid); ok {
			clerk := shardgrp.MakeClerk(ck.clnt, servers)
			if value, version, err = clerk.Get(key); err != rpc.ErrWrongGroup {
				return
			}
		}
		time.Sleep(100 * time.Millisecond) // when the shardgrp isn't responsible for the shard
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) (err rpc.Err) {
	var ok bool
	var servers []string
	var shid shardcfg.Tshid
	var cfg *shardcfg.ShardConfig

	for {
		cfg = ck.sck.Query()
		shid = shardcfg.Key2Shard(key)
		if _, servers, ok = cfg.GidServers(shid); ok {
			clerk := shardgrp.MakeClerk(ck.clnt, servers)
			if err = clerk.Put(key, value, version); err != rpc.ErrWrongGroup {
				return err
			}
		}
		time.Sleep(100 * time.Millisecond) // when the shardgrp isn't responsible for the shard
	}
}
