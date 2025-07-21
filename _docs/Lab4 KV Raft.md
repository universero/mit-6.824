
# Lab4 KV Raft
___
## Introduction

在本实验中，你将基于Lab3完成的Raft服务构建一个容错的KV存储。 对于客户端来说，该服务看起来类似于Lab2 。不过，该服务并非由单个服务器组成，而是由一组使用 Raft 来帮助它们维护相同数据库的服务器组成。只要大多数服务器处于活动状态且能够通信，即使出现其他故障或网络分区，你的KV服务也应该能够持续处理客户端请求。完成实验 4 后，您将实现 [Raft 交互图](https://pdos.csail.mit.edu/6.824/figs/kvraft.pdf)中所示的所有部分（Clerk、Service 和 Raft）。

客户端会通过Clerk来与KV服务交互, 使用与Lab2语义一致的Put和Get方法, Put最多执行一次, 且Puts/Gets需要形成线性的历史记录

对于单服务器来说，提供线性一致性相对容易。但如果服务是复制的，则难度会更大，因为所有服务器都必须对并发请求选择相同的执行顺序，必须避免使用非最新状态回复客户端，并且必须在故障后以保留所有已确认的客户端更新的方式恢复其状态。

本实验分为三个部分。在 A 部分，你将使用你的 Raft 实现实现一个复制状态机包 rsm ； rsm 对其复制的请求无关。在 B 部分，你将使用 rsm 实现一个复制KV服务，但不使用快照。在 C 部分，你将使用你在实验 3D 中实现的快照实现，这将允许 Raft 丢弃旧的日志条目。
## Getting Started

我们在 src/kvraft1 中提供框架代码和测试。框架代码使用框架包 src/kvraft1/rsm 来复制server。server必须实现 StateMachine 接口才能使用 rsm 进行自我复制。你的大部分工作将是实现 rsm ，以提供复制功能。您还需要修改kvraft1/client.go 和 kvraft1/server.go 来实现有关server的部分。这种拆分允许你在下一个实验中重复使用 rsm 。你可以重复使用实验 2 中的部分代码，但这不是必需的。
## Part A: replicated state machine

在使用Raft进行复制的常见客户端/服务器服务场景中，服务以两种方式与Raft交互：服务leader通过调用raft.Start()提交客户端操作，所有服务副本通过Raft的applyCh接收已提交的操作并执行它们。在leader节点上，这两个活动会相互影响。在任何给定时间，一些服务器goroutine正在处理客户端请求，已经调用了raft.Start()，每个都在等待其操作被提交并获取操作执行结果。当已提交的操作出现在applyCh上时，每个操作都需要由服务执行，并且需要将结果传递给调用raft.Start()的goroutine，以便它能将结果返回给客户端。

rms包封装了上述的交互, 它位于service和raft之间的一层.在rsm.go中你需要实现一个用于读取applyCh的reader goroutine, 以及一个`rsm.Submit()`函数, 它为客户端操作调用`raft.Start()`然后等待reader goroutine将操作结果返回.

使用rsm的service对于rsm reader goroutine来说, 是一个提供`DoOp`方法的`StateMachine`. reader将每个已提交的操作交给`DoOp()`; `DoOp()`的返回值应该提供给对于的`rsm.Submit()`. `DoOp()`的参数和返回值是any类型, 实际值与服务传递给rsm.Submit的参数和返回值具有相同类型

service应该将每个客户端操作传递给`rsm.Submit()`,为了帮助reader匹配applyCH消息和对应的`rsm.Submit()`, Submit应该将每个客户端操作封装为具有唯一标识的Op结构体. 然后Submit应该等到操作提交并执行完毕, 并返回执行结果. 如果raft.Start()反馈当前peer不是Raft leader, 则Submit应该返回`rpc.ErrWrongLeader`错误. Submit应该检查并处理在调用Start后领导权立即发生变化从而导致操作丢失的情况

在PartA中, rsm tester充当service, 提交的操作会被解释为对单个整数组成的状态进行增量操作. 在PartB中, 你将把rsm作为实现StateMachine的service的一部分, 并调用rsm.Submit()

如果一切顺利, 客户端请求的时间顺序如下:
- client向service leader发送请求
- service leader调用rsm.Submit()
- Submit调用Start并等待
- Raft提交请求并发送到所有peer的applyCh
- 每个peer上的rsm reader读取applyCh的请求并传递给DoOp()
- 在leader上, rsm reader将DoOp()返回给对应的Submit

Task: 实现rsm.go: Submit方法和reader goroutine; 并通过4A test
### Hint
- 虽然不需要, 但是你允许在ApplyMsg或Raft RPC中增加字段
- 你需要处理已调用Start提交Submit请求但在请求提交到日志之前就失去领导权的leader. 实现这一点的方法之一是发现Raft的term改变, 或者在Start()返回的index上发生了不同的操作, 并从Submit返回rpc.ErrWrongLeader. 如果前leader处于分区中, 他将无法感知新的leader; 但任意一个相同分区中的客户端无法也无法和新的leader通信, 这种情况下service可以无限等待直到分区恢复.
- 当tester调用rf.Kill来关闭peer时, Raft应该close(applyCh), 从而使得rsm了解关闭情况, 并退出所有循环.
### 实验过程

这个实验还是比较直接的, 实现一个reader, 一个Submit即可
```go
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// submit op and wait for weak up
	rsm.mu.Lock()
	index, term, isLeader := rsm.rf.Start(req) // try to submit the op
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	op := &Op{Id: index, Me: rsm.me, Term: term, Req: req, Done: make(chan struct{})}
	//fmt.Printf("[rsm %d] [term %d] submit op %+v\n", rsm.me, term, op)
	rsm.waits[op.Id] = op
	rsm.mu.Unlock()
	for {
		select {
		case <-op.Done:
			if term != op.Term {
				return rpc.ErrWrongLeader, nil
			}
			return op.Err, op.Resp
		case <-time.After(100 * time.Millisecond):
			if currentTerm, stillLeader := rsm.rf.GetState(); currentTerm == term && stillLeader {
				continue
			}
			return rpc.ErrWrongLeader, nil
		}
	}
}
```
需要按照提示对submit提交后leader改变的特殊情况做一下判断
同时需要做个周期性的判断, 当不再是leader后, 不能无限等待. 因为leader转换后, 后续的操作永远不会提交, reader没有机会通知对应的Submit, 只能靠Submit自己关闭
```go
func (rsm *RSM) reader() {  
    var term int  
    var resp any  
    for {  
       select {  
       case msg, ok := <-rsm.applyCh: // receive a finish op  
          if !ok {  
             return  
          }  
          rsm.mu.Lock()  
          if msg.Command != nil {  
             resp = rsm.sm.DoOp(msg.Command) // 执行操作  
             if op, exist := rsm.waits[msg.CommandIndex]; exist {  
                term, _ = rsm.rf.GetState()  
                op.Err, op.Resp = rpc.OK, resp  
                //fmt.Printf("[rsm %d] [term %d] submit msg %+v with error %s \n", rsm.me, term, msg, op.Err)  
                op.Term = term  
                close(op.Done)  
                delete(rsm.waits, msg.CommandIndex)  
             }  
          }  
          rsm.mu.Unlock()  
       }  
    }  
}
```
需要保证Command不是nil
## PartB KV service without snapshots

现在需要用rsm来实现一个kv server. 每个server都有一个对应的rsm/raft peer. Clerk 发送Put/Get RPC请求来将操作提交给rsm, rsm通过Raft复制该操作, 并在每个peer上调用server的DoOp, DoOp将操作应用到peer的kv 数据库中, 以此让每个server维护完全相同的副本

Clerk有时无法确定哪个是Raft的Reader, 如果Clerk向错误的server发送RPC或者无法连接到server, 应该尝试请求其他的kvserver. 如果kv server将操作提交到日志, leader会通过响应RPC请求将结果告知Clerk. 如果操作失败则返回错误, 然后Clerk尝试其他的服务器

首要任务是实现一个可靠网络且server没有故障的方案, 可以随意的复制Lab2的代码, 需要添加逻辑来决定RPC发送到哪个server
还需要在server.go中实现Put和Get的RPC handler, 这些handler会将对应的请求提交给Raft, 且需要实现DoOp方法供rsm调用
通过TestBasic4B说明完成了第一个任务

如果server不属于majority, 则不应该完成Get以免提供过时数据
最好一开始就添加lock, 有时避免死锁会影响代码设计

然后修改你的方案, 以便在网络或server故障的情况下继续正常运行. 你会面临的一个问题是Clerk可能需要多次发送RPC直到kvserver积极地回复. 如果leader在向日志提交后失败, 可能不会回复, 因此需要将请求发送给另一位leader. 每次调用Put应该保证特定版号只执行一次

第二个任务就是添加代码来处理错误, 你可以使用与Lab2类似的重试方法, 包括在重试的Put响应丢失时返回ErrMaybe, 需要通过go test -v -run 4B

当rsm leader变成follower时, Submit会响应ErrWrongLeader, 这种情况下应该将请求重新发送到其他服务器直到找到新的leader.
可能需要修改Clerk使其记住上一次RPC中哪个服务器是leader, 并在下一次RPC时优先向这个服务器发送, 减少leader的搜索时间
### 实验过程
整体和实验2其实都还是比较类似的
```go
type Clerk struct {  
    clnt    *tester.Clnt  
    servers []string  
    mu      sync.Mutex  
    recent  atomic.Int32 // 最近一次的leader  
    n       int32  
    seq     atomic.Int32  
    me      string  
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {  
    args, reply := &rpc.GetArgs{Key: key}, rpc.GetReply{}  
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
    args, reply := &rpc.PutArgs{Key: key, Value: value, Version: version, Seq: seq, CkId: ck.me}, rpc.PutReply{}  
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
          return rpc.ErrMaybe  
       }   
       return reply.Err  
    }  
}
```
Clerk的Get和Put都和Lab2基本一样, 只是多了一步尝试不同的server, 以及每个put命令需要唯一标识
```go
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
    key     string  
    value   string  
    version rpc.Tversion  
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
  
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {  
    var resp any  
    err, resp := kv.rsm.Submit(args)  
    if err != rpc.OK {  
       reply.Err = err  
       return  
    }  
    *reply = *(resp.(*rpc.GetReply))  
    fmt.Printf("[kvserver %d] Get Reply: %+v\n", kv.me, reply)  
}  
  
func (kv *KVServer) doGet(args *rpc.GetArgs) (reply *rpc.GetReply) {  
    kv.mu.Lock()  
    defer kv.mu.Unlock()  
  
    //fmt.Printf("[kvserver %d] Get Args: %+v\n", kv.me, args)  
    reply = &rpc.GetReply{Err: rpc.ErrNoKey}  
    if kvv, ok := kv.kvs[args.Key]; ok {  
       reply.Err = rpc.OK  
       reply.Value = kvv.value  
       reply.Version = kvv.version  
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
    fmt.Printf("[kvserver %d] Put Reply: %+v\n", kv.me, reply)  
}  
  
func (kv *KVServer) doPut(args *rpc.PutArgs) (reply *rpc.PutReply) {  
    kv.mu.Lock()  
    defer kv.mu.Unlock()  
  
    fmt.Printf("[kvserver %d] Put Args: %+v\n", kv.me, args)  
    if last, ok := kv.ckSeq[args.CkId]; ok && last.Seq >= args.Seq { // 做过的请求  
       reply = &rpc.PutReply{Err: last.Err, Seq: last.Seq}  
       return  
    }  
    defer func() { kv.ckSeq[args.CkId] = reply }()  
    reply = &rpc.PutReply{Err: rpc.OK, Seq: args.Seq}  
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
  
func (kv *KVServer) Kill() {  
    atomic.StoreInt32(&kv.dead, 1)  
    // Your code here, if desired.  
    //fmt.Printf("[kvserver %d] %++v\n", kv.me, kv.kvs)}  
  
func (kv *KVServer) killed() bool {  
    z := atomic.LoadInt32(&kv.dead)  
    return z == 1  
}  
 
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {  
    // call labgob.Register on structures you want  
    // Go's RPC library to marshall/unmarshall.    labgob.Register(rsm.Op{})  
    labgob.Register(rpc.PutArgs{})  
    labgob.Register(rpc.GetArgs{})  
    kv := &KVServer{me: me}  
  
    kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)  
    kv.kvs = make(map[string]*tuple)  
    kv.ckSeq = make(map[string]*rpc.PutReply)  
    return []tester.IService{kv, kv.rsm.Raft()}  
}
```
Put的话, 需要维护每个clerk的最后一次请求seq. 拒绝重复的请求
这里有个坑花了很久时间, follower提交的Command是值类型而leader提交的Command是指针类型, DoOp里一开始只考虑了指针类型, 导致follwer的kv对一直为空. 出现这个原因应该是因为AppendEntries请求中发送方和接收方在gob注册的Command类型不同
## PartC:

这一部分中需要修改kvserver和rsm, 使得能够使用Raft的Snapshot()实现的快照.
tester将maxraftstate传递到StartKVServer()中, 并传递给rsm. 当rf.PersistBytes大小接近maxraftstate时, 应该调用Snapshot来保存快照. rsm可以通过调用StateMachine的Snapshot方法来获取快照, 如果maxraftstate为-1则不用创建快照. maxraftstate限制Raft传递给persister.Save()的GOB-encoded字节.

Task1: 修改rsm使得当之持久化的状态过大时, 提交快照. 当rsm服务器重启时, 使用ReadSnapshot读取快照, 如果快照长度大于0则传递给Restore(). rsm需要通过RestSnapshot4C以完成此任务.
Task2: 实现server中的Snapshot和Restore
## 实验过程
这个实验算是非常简单的里, 代码少思路也简单
rsm启动时读取一下snapshot, 每完成一个命令后都判断一下持久化数据是不是太大了, 收到snapshot时就应用一下
```go
// MakeRSM
if snap := persister.ReadSnapshot(); len(snap) > 0 {  
    rsm.sm.Restore(snap)  
}

// reader
if msg.CommandValid && msg.Command != nil {  
    //fmt.Printf("[rsm %d] doing %+v \n", rsm.me, msg.Command)  
    resp = rsm.sm.DoOp(msg.Command) // 执行操作  
    if op, exist := rsm.waits[msg.CommandIndex]; exist {  
       term, _ = rsm.rf.GetState()  
       op.Err, op.Resp = rpc.OK, resp  
       //fmt.Printf("[rsm %d] [term %d] submit msg %+v with error %s \n", rsm.me, term, msg, op.Err)  
       op.Term = term  
       close(op.Done)  
       delete(rsm.waits, msg.CommandIndex)  
    }  
    if rsm.maxraftstate != -1 && rsm.maxraftstate-rsm.rf.PersistBytes() < 20 {  
       rsm.rf.Snapshot(msg.CommandIndex, rsm.sm.Snapshot())  
    }  
} else if msg.SnapshotValid && len(msg.Snapshot) > 0 {  
    rsm.sm.Restore(msg.Snapshot)  
}

// server.go
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
```
这里需要注意的是gob要求编码的字段都是大写导出的