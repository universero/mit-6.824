> 实验要求: [Key/Value Server](https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv1.html)
___
## Introduction

在这个实验中, 你需要构建一个单机的key/value server, 确保每个Put操作最多执行一次, 即使网络发生错误, 并且操作是线性的. 你将会用这个KV server实现一个锁, 后续的labs会重复一个像这样的server来解决crashes
## KV server

每个客户端使用Clerk来和key/value server交互, Clerk复制向服务器发送RPC请求.
Clients可以向Server发送两个不同的RPC请求: `Put(key,value,version)`, `Get(key)`. Server维护一个内存中的map来记录每个(key, value, version)三元组, key和value是字符串, version记录key被写的次数.

`Put(key, value, version)`方法只有在version匹配server上key的version时才会执行put操作, 然后增加key的版本. 如果不匹配则会返回`rpc.ErrVersion`. 客户端可以通过put一个key和version 0来创建一个新的key(然后server会存储的version是1). 如果版本大于0但是key不存在, server会返回`rpc.ErrNoKey`

`Get(Key)`获取key的现有值和对应的版本, 如果key不存在, 则server会返回`rpc.ErrNoKey`.

当网络不可靠且客户端重传时, 维护一个version对于实现锁和最多执行一次是非常有效的

当你完成了Lab并通过所有测试后, 从客户端执行Clerk.Get和Clerk.Put的角度看, 你将会得到一个线性化的kv service. 也就是说, 如果客户端的操作不是并行的, 每个客户端的`Clerk.Get`和`Clerk.Put`都会观察到先前的操作序列所导致的状态修改, 对于并行操作, 返回值和最终状态都会和按照某种序列顺序执行的结果一致. 如果在操作在事件上重叠, 一个操作必须能观测到在该操作开始前已结束的所有操作的影响

线性对于应用程序来说是方便的, 因为他就像单个服务器每次处理单个请求时的行为一样.
## Getting Started

代码框架在`src/kvsrv1`中. `kvsrv1/client.go`实现了管理RPC交互的Clerk, Clerk提供Put和Get方法. `kvsrv1/server.go`包含了服务端代码, 包括Put和Get的handler. 你需要修改client,go和server,go. 请求, 响应和错误定义在了`kvsrv1/rpc/rpc.go`中, 这个文件不能修改.
## Key/Value Server With Reliable Network (essay)

第一个任务是实现客户端不会丢失信息情况下KV Server. 你需要完成Clerk Put/Get的RPC发送代码和server的Put和Get实现代码. 这一个任务需要通过`go test -v -run Reliable`
每个Passed后面的数字分别是实时事件, 常数1, 发送的RPC数量(包括Get和Pur)
### 过程分析
#### client
Get方法要求如下:
- 如果获取到ErrNotKey则返回
- 如果获取到其他错误则一直重试
  由此可以编写出可靠环境下的Get方法
```go
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := &rpc.GetArgs{Key: key}
	reply := &rpc.GetReply{}
	// 重复直到成功
	for !ck.clnt.Call(ck.server, "KVServer.Get", args, reply){
	}
	return reply.Value, reply.Version, reply.Err
}
```

Put方法要求如下:
- 第一次收到ErrVersion, Put应该返回ErrVersion, 因为可以明确没有在Server上执行put
- 如果是第二次收到ErrVersion, Put需要返回ErrMaybe, 因为先前的RPC可能已经执行了, 但是响应丢失了
  可以编写出可靠环境下的Put方法. Clerk中需要加一个versionErr字段统计错误出现次数
```go
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := &rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := &rpc.PutReply{}

	for !ck.clnt.Call(ck.server, "KVServer.Put", args, reply){
	}
	if reply.Err == rpc.ErrVersion {
		ck.versionErr++
	}
	// 第二次版本错误
	if ck.versionErr >= 2 {
		ck.versionErr = 0
		return rpc.ErrMaybe
	}
	return reply.Err
}
```
#### server
kv的存储, 很容易想到用map, 这里定义了一个三元组的map来存储所有的kv对
```go
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
```
get的要求比较简单
- 存在就返回内容, 不存在就返回ErrNoKey
```go
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	reply.Err = rpc.ErrNoKey
	if kvv, ok := kv.kvs[args.Key]; ok {
		reply.Err = rpc.OK
		reply.Value = kvv.value
		reply.Version = kvv.version
	}
	return
}
```

put的要求是
- key存在且版本匹配则更新
- 不存在且参数中版本为0则增加
  需要注意的是对map写时需要加锁
  可以写出如下代码
```go
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
		kv.kvs[args.Key] = &tuple{args.Key, args.Value, args.Version + 1}
	} else { // 不存在且版本不为0
		reply.Err = rpc.ErrNoKey
	}
	return
}
```

运行`go test -v -run Reliable`测试可靠网络环境下的KV Server, 通过测试
## Implementing a lock using key/value clerk

在许多系统中, client运行在不同的机器上使用kv server来协调他们的活动, 例如Zookeeper和Etcd允许客户端使用分布式锁去协作, 他们通过条件put实现了这样的锁

在这个任务中, 你需要实现一个layered on(词典意思是穿多层衣服, 感觉是笔误了, 应该是relied on)Clerk.Put和Clerk.Get调用. 这种锁支持两种方法, Acquire和Release. 在他同一时刻只有一个客户端可以获取锁, 其他的客户端需要等待直到该客户端释放锁,

代码框架在`src/kvsrv1/lock`中, 你需要修改lock.go, 你的Acquire和Release可以通过lk.ck.Put()和lk.ck.Get()来与kv server交互

如果一个客户端在获取锁的过程中宕机了, 这个锁永远不会被释放. 在比这个实验更复杂的设计中, 客户端会给锁加一个lease(租约). 当租约过期, server会代替client释放锁. 在这个实验中你可以忽视这个问题

需要通过在`lock`目录下的`go test -v run Reliable`

这个任务需要的代码比较少, 但是比之前的实验需要更多的独立思考
- 每个lock client需要一个独特的标识符, 用`kvtest.RandValue(8)`来生成随机的字符串
- lock service一个使用特定的key来存储锁状态(你需要精确的确定锁的状态). 会被用到的key通过MakeLock的参数`l`来传递.
### 过程记录
每个`l`作为key, 表示被加锁的资源, 每个value用lk.id, 表示锁的持有者
```go
type Lock struct {
	ck kvtest.IKVClerk
	id string // 持有者ID
	l  string // 锁的资源
}
```
#### 加锁
加锁时首先应该获取锁, 如果自己已经持有了锁, 那么应该之间退出.
如果没有锁, 则应该尝试加锁. 如果加锁失败, 应该重新整个流程, 避免加锁过程中已经有其他的客户端加锁.
这里用了个常量unlock表示没有加锁的状态
```go
func (lk *Lock) Acquire() {
	for {
		// 自旋获取key
		value, version, err := lk.ck.Get(lk.l)
		if value == lk.id { // 已持有锁
			return
		} else if err == rpc.ErrNoKey || value == unlock { // 未加锁
			// 尝试加锁
			if err = lk.ck.Put(lk.l, lk.id, version); err == rpc.OK {
				return
			}
		}
	}
}
```
#### 解锁
解锁是加锁的逆过程, 先获取锁, 如果没有持有锁, 直接返回
如果持有锁, 就尝试释放锁.
```go
func (lk *Lock) Release() {
	for {
		// 自旋获取key
		value, version, err := lk.ck.Get(lk.l)
		if value != lk.id || err == rpc.ErrNoKey { // 未持有锁
			return
		} else { // 尝试释放锁
			if err = lk.ck.Put(lk.l, unlock, version); err == rpc.OK {
				return
			}
		}
	}
}
```
## Key/Value Server With Dropped Messages

这个任务最难的部分在于网络可能重排序, 延迟或者丢失RPC请求或者响应. 为了从丢失中恢复, Clerk必须持续尝试, 直到它接收到Server的响应.

如果网络丢失后client请求, 那么重发就可以解决消息丢失的问题.
但是网络可能丢失RPC响应, client重复会导致执行重复的操作. 对于Get来说可能没有影响, 但是持有相同的Version的Put会导致server返回rpc.ErrVersion而不是再次执行Put.

一种棘手的情况是如果server响应了rpc.ErrVersion, Clerk不知道put是否成功执行. 可能先前的请求已经正确Put或者其他的Clerk先一步完成了Put. 所以之前的任务中, 第一次ErrVersion直接返回, 第二次则返回ErrMaybe.

应用程序的开发肯定希望, Put能精确地只执行一次(不返回ErrMaybe), 但如果不在server上维持每一个Clerk的状态, 这很难完成. 在这个任务中, 你需要实现用你的Clerk实现一个锁来探索如何实现最多执行一次的Clerk.Put.

现在你需要修改`kvsrv1/client.go`来面对RPC请求和响应的丢失问题. ck.clnt.Call()的返回值标识了是否正确接收到RPC响应. 你的Clerk应该保持重发直到接收到响应. 你的方案不应该修改server
需要通过 `go test -v`
### 过程记录
#### Get
没什么要改的, 加个失败后等待
```go
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {  
    args := &rpc.GetArgs{Key: key}  
    reply := &rpc.GetReply{}  
    // 重复直到成功  
    for !ck.clnt.Call(ck.server, "KVServer.Get", args, reply) {  
       time.Sleep(100 * time.Millisecond)  
    }  
    return reply.Value, reply.Version, reply.Err  
}
```
#### Put
和前面的差不多, 只不过重试的概念不一样, 前面写成了两次ErrVersion, 而这里是有多次没收到
```go
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

```
### Implementing a lock using key/value clerk and unreliable network

由于前面想的比较多, 这里的任务其实已经覆盖了, 什么都不用改就行