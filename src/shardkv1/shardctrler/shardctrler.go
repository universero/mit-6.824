package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
	"sync"
)

var nowCfg = "now-config"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// InitController The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// InitConfig Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	strCfg := cfg.String()
	if err := sck.IKVClerk.Put(nowCfg, strCfg, rpc.Tversion(0)); err != rpc.OK {
		panic("init config err" + err)
	}
}

// ChangeConfigTo Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	var toFroze []shardcfg.Tshid
	var old *shardcfg.ShardConfig
	var version rpc.Tversion
	oldCache := make(map[shardcfg.Tshid][]byte)

	if strCfg, ver, err := sck.IKVClerk.Get(nowCfg); err == rpc.OK {
		version = ver
		old = shardcfg.FromString(strCfg)
	} else {
		panic("no old config")
	}
	DPrintf("[controller] find old config \n%+v\n and receive new config \n%+v\n", old, new)
	for shid, gid := range new.Shards {
		if old.Shards[shid] != gid { // different map from shard to shardgrp
			toFroze = append(toFroze, shardcfg.Tshid(shid))
		}
	}
	DPrintf("[controller] shard to freeze %+v\n", toFroze)

	// froze
	for _, shid := range toFroze {
		wg.Add(1)
		go func(shid shardcfg.Tshid) {
			defer wg.Done()
			if gid, server, ok := old.GidServers(shid); ok {
				clerk := shardgrp.MakeClerk(sck.clnt, server)
				// froze the shard and get the state
				var cache []byte
				var err rpc.Err
				if cache, err = clerk.FreezeShard(shid, new.Num); err != rpc.OK {
					panic("freeze error:")
				}
				DPrintf("[controller] shard %d frozed on group %d\n", shid, gid)
				mu.Lock()
				defer mu.Unlock()
				oldCache[shid] = cache
			}
		}(shid)
	}
	wg.Wait()
	//fmt.Printf("[controller] old cache \n%+v\n", oldCache)
	// install
	for _, shid := range toFroze {
		wg.Add(1)
		go func(shid shardcfg.Tshid) {
			defer wg.Done()
			if gid, server, ok := new.GidServers(shid); ok {
				clerk := shardgrp.MakeClerk(sck.clnt, server)
				mu.Lock()
				cache := oldCache[shid]
				mu.Unlock()
				if err := clerk.InstallShard(shid, cache, new.Num); err != rpc.OK {
					panic("install shard error:" + err)
				}
				DPrintf("[controller] shard %d installed on group %d\n", shid, gid)
			} else {
				panic("no such shard")
			}
		}(shid)
	}
	wg.Wait()

	// delete
	for _, shid := range toFroze {
		wg.Add(1)
		go func(sid shardcfg.Tshid) {
			defer wg.Done()
			if gid, server, ok := old.GidServers(sid); ok {
				clerk := shardgrp.MakeClerk(sck.clnt, server)
				if err := clerk.DeleteShard(shid, new.Num); err != rpc.OK {
					panic("delete shard error")
				}
				DPrintf("[controller] shard %d deleted on group %d\n", shid, gid)
			}
		}(shid)
	}
	wg.Wait()

	strCfg := new.String()
	if err := sck.IKVClerk.Put(nowCfg, strCfg, version); err != rpc.OK {
		panic("change config err" + err)
	}
}

// Query Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	if strCfg, _, err := sck.IKVClerk.Get(nowCfg); err == rpc.OK {
		return shardcfg.FromString(strCfg)
	}
	panic("query err")
}
