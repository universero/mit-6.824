package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"time"
)

var unlock = "unlock"

type Clerk struct {
	clnt       *tester.Clnt
	server     string
	versionErr int
	id         string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server, versionErr: 0, id: kvtest.RandValue(8)}
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := &rpc.GetArgs{Key: key}
	reply := &rpc.GetReply{}
	// 重复直到成功
	for !ck.clnt.Call(ck.server, "KVServer.Get", args, reply) {
		reply = &rpc.GetReply{}
		time.Sleep(100 * time.Millisecond)
	}
	//fmt.Printf("[get] |key: %s|value: %s|gversion: %d|err: %s\n", args.Key, reply.Value, reply.Version, reply.Err)
	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := &rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := &rpc.PutReply{}
	retries := 0
	for {
		for !ck.clnt.Call(ck.server, "KVServer.Put", args, reply) {
			retries++
			time.Sleep(100 * time.Millisecond)
		}
		if retries > 0 && reply.Err == rpc.ErrVersion {
			return rpc.ErrMaybe
		}
		return reply.Err
	}
}
