# Lab3 Raft
> 实验要求: [Lab Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft1.html)
___
## Introduction

这是构建有容错能力的kv存储系统一系列Lab中的第一个, 这这个Lab中你需要实现Raft, 一个复制状态的协议. 在下一个实验中你需要构建基于Raft的kv服务. 然后你将会"shard"(分片)你的服务到多个replicated state的机器上以获得高性能

一个replicated服务通过在多个replica servers复制它的状态来实现容错. Replication允许service在部分server出错的情况下继续操作, 这里会面临的挑战是failures可能导致replicas持有数据的不同副本.

Raft将组织client请求组织为一个称作log的序列, 并且确保所有的replica会看见同一个log. 每个replica按照log的顺序执行client请求, 将他们应用在本地service状态的副本上. 因为所有存活的replicas会看到相同的log内容, 所有他们会按相同的顺序执行相同的请求, 从而继续保持相同的service状态. 如果一个server出错但后续恢复了, Raft会将它的log更新为最新状态. 只要大部分的server是存活的且可以互相通信, Raft将继续执行. 如果大部分不是存活且可通信的, Raft将不会继续执行, 但是一旦大多数服务器能再次上线, 它就会从中断处继续运行.

在本实验中, 你将实现Raft为Go对象类型, 并附带相关方法, 意在用作一个大型服务的模块. 一组Raft实例通过RPC相互通信以维护复制的日志. 你的Raft接口需要支持一个无限的带有编号的命令序列, 也叫做log entries. entries使用index number编号. 被给定编号的log entry最终会提交, 在那时候, 你的Raft应该将log entry发送到更大的服务以供其执行

你应该遵循extended Raft paper的设计, 尤其是Figure 2, 你需要实现论文中大部分内容, 包括保存持久状态和在节点故障重启后读取他们. 你不会实现集群成员变更(section 6)

## Getting Started

代码框架在`src/raft/raft,go`中, 且提供了一系列的推动你实现工作的测试.

你的实现必须支持如下的接口, tester和你最终的kv server会使用, 你可以在raft.go中找到更多的细节
```go
// create a new Raft server instance:
rf := Make(peers, me, persister, applyCh)

// start agreement on a new log entry:
rf.Start(command interface{}) (index, term, isleader)

// ask a Raft for its current term, and whether it thinks it is leader
rf.GetState() (term, isLeader)

// each time a new entry is committed to the log, each Raft peer
// should send an ApplyMsg to the service (or tester).
type ApplyMsg
```
一个service会调用`Make(peers,me,...)`来创建一个peer. peers的参数是一组用于RPC通信的Raft peer的网络标识符(包括这个). me参数是这个peer在peers数字中的索引. `Start(command)`请求Raft开始运行, 将命令附加到replicated log中, Start应该立即返回而不是等待日志附加完成. service希望你的实现对每个新提交的log entry发送一个ApplyMag到applyCh channel参数传递到Make()中.

raft.go中包含了RPC发送和处理的示例(`sendRequestVote()`和`RequestVote()`), 你的Raft peer应该使用labrpc包交换PRC. tester可以告诉labrpc来延迟, 重新排序以及丢弃RPC来模拟网络错误. 尽管你可以临时修改labrpc, 但是你需要确保你的Raft在原始的labrpc上能正常工作. 你的Raft实例必须使用RPC通信而不能使用其他的方法.

## Part 3A: leader election

实现Raft leader选举和心跳机制(AppendEntry 无log entries的RPCs) Part3A的目标是选举一个leader, 如果没有错误发送则这个leader保持leader的身份, 如果老的leader故障了或者它的包丢失了, 就应该选举一个新的leader来接管.
运行`go test -run 3A`来测试

每个Passed后面包含五个数字: 测试耗时, Raft Peers数量, 测试发送的RPC数量, RPC消息总字节数以及Raft报告的已提交entry数. 测试总耗时超过600s或单个测试超过120s则判断您的解决方案失败.

### Hint

- 你不能简单的直接运行Raft, 而是通过test程序来运行
- 根据论文中Figure 2, 此时你需要关心的RequestVote RPC的发送接收, 与选举相关的服务器规则和与领导者选举相关的状态
- 补充RequestVoteArgs和RequestVoteReply的参数. 修改Make,  创建一个background goroutine, 当一段时间内没有收到其他peer的消息时, 周期性地发出RequestVote发起leader选举. 实现RequestVote RPC handler以便服务器之间相互投票
- 为了实现心跳, 定义一个AppendEntries RPC结构(尽管你可能目前不需要所有的参数), 并让leader周期性发送的. 实现AppendEntries RPC handler方法.
- tester要求Leader每秒发送的heartbeat不超过10次
- tester要求需要在五秒内选出新的leader
- 论文section 5.2提到每次选举时间在150ms到300ms之间, 但只有在leader发送heartbeat的频率高于每150ms一次时才有意义, 由于tester限制每秒发送十次heartbeat, 因此你必须使用大于150ms到300ms的选举超时时间, 但是也别太大, 不然可能无法在5s内选出leader
- Go的rand库会比较有用
- 当你需要编写定期执行或延迟一定时间后执行的代码时, 最简单的方法是创建一个带有循环的goroutine, 然后调用time.Sleep. 可以参考Make()创建的`ticker()`. 不要使用time.Timer或time.Ticker, 它们很难被正确使用
- 不要忘记实现`GetState()`
- 当需要永久关闭实例时, tester会调用`rf.Kill()`. 你可以使用`rf.killed()`来检查是否调用过`Kill()`. 你可能需要在所有的循环中执行这个操作, 避免死亡的Raft实例打印令人困惑的消息
- 如果测试失败, tester会生成一个文件, 将各种事件可视化, 包括网络分区, 崩溃的服务器和已执行的检查. 如下是[可视化的示例](https://pdos.csail.mit.edu/6.824/labs/vis.html). 此外你可以通过如下方式添加自己的注释, `tester.Annotate("Server 0", "short description", "details")`.
## Part 3B: log

完成leader和follower直接添加Log Entries的代码, 需要通过`go test -run 3B`
如果您的解决方案在 3B 测试中实际耗时超过一分钟，或者 CPU 时间超过 5 秒，您以后可能会遇到麻烦。请查找休眠或等待 RPC 超时的时间、未休眠或等待条件或通道消息而运行的循环，或者发送的大量 RPC。

### Hint
- Raft的日志是从1开始的, 但是这里建议从0开始(第一个日志是index-0, term-0). 这使得第一个AppendEntries RPC包含prevLogIndex, 并成为日志中的有效索引
- 首要目标是通过3B测试, 首先实现Start, 然后通过AppendEntries发送和接收条目. 每个peer都需要发送每个新提交的entry到applyCh
- 你需要实现选举限制
- 你的代码中可能有重复检查某些事件的循环, 不要自旋, 用time.Sleep或条件变量
## Part 3C: persistence

如果基于Raft的服务重启, 它应该从上次中断的地方恢复服务, 这要求Raft能保持重启后仍能继续运行的持久状态. Figure 2中说明了哪些字段应该被持久化.

实际的实现会在Raft持久状态发生变化时将其写入磁盘, 并在重启后从磁盘读取状态. 在lab里, 你的实现不会实际使用磁盘, 相反, 他会从Persister对象保存和恢复持久状态. 调用`Raft.Make()`的用户会提供一个保存了Raft最近的持久状态的Persister. Raft应该在Make中初始化其状态, 并在每次状态发生变化时都使用`ReadRaftState()`和`Save()`来保存状态.

你的任务是完成函数`persist()`和`readPersist()`来保存和恢复持久状态. 你需要编码或序列化状态为一个字节数组以将其传递给Persister. 请使用labgob编码器, 并参考`persist()`和`readPersist()`中的注释. labgob类似于Go的gob编码器, 但如果你尝试编码有小写字段的结构体则会报错. 目前, `persister.Save()`第二个参数使用`nil`作为参数, 在您的实现更改持久状态的地方插入对`persist()`的调用。完成这些之后，如果实现的其余部分都是正确的，那么您应该可以通过所有3C测试。
## Parc 3D: log compaction

对于长期运行的服务来说, 重启时读取完整的Raft日志是不合理的. 你需要修改Raft的协作方式, 不时地存储状态快照, 此时Raft会丢弃快照之前的日志条目. 这样可以让重启变得更为快速, 但是这样follower可能会落后太多以至于丢失了leader以抛弃的条目, 所以leader现在必须要发送snapshot.

你需要实现`Snapshot(index int, snapshot []byte)`函数, 服务可以使用这个来序列化快照.

在Lab3D, tester会周期性地调用Snapshot. 在Lab 4中你会编写一个调用`Snapshot()`的KV服务;snapshot需要包含完整的键值对表, 服务层会在每个peer上调用`Snapshot()`
参数`index`表示快照中最高的条目, Raft应该丢弃index之前的日志.
你需要实现InstallSnapshot RPC, 该RPC允许leader通知滞后的Raft peer使用快照来替换他的状态.
当follower收到InstallSnapshot RPC时, 它可以使用applyCh将快照以ApplyMsg的形式发送到服务.这些快照只会推进服务的状态不会导致其后退
Raft应该保留Raft状态会对于的快照, 使用persister.Save的第二个参数来保存快照.

你的任务是实现 Snapshot() 和 InstallSnapshot RPC，并对 Raft 进行一些修改以支持这些功能（例如，使用修剪日志进行操作）。当您的解决方案通过 3D 测试（以及之前的所有实验 3 测试）后，即视为完成。
### Hint
- 一个好的开始是修改代码, 使其能仅存储从索引X开始的部分. 首先你可以将X设置为0, 然后允许3B/3C测试, 然后让Snapshot(index)丢弃index之前的日志, 并将X设置为index
- 第一个3D测试失败的常见原因是follower花了太长的实际来跟上leader
- 下一步, 如果没有时follower更新所需要的日志条目, 则发送InstallSnapshot
- 通过单个InstallSnapshot RPC更新快照, 不要实现Figure 13中的offset
- 不要保留存入快照中的日志的引用, 否则无法进行垃圾收集
- 不使用race, 完成所有的测试合理的实际是6分钟实际时间和2分钟CPU; 加上race应该10分钟实际时间和2分钟CPU时间
## 实验过程

本来是打算详细记录下, 但是写了一半就放弃了. 整个协议看起来不复杂, 但是实际上有非常多细节部分需要注意, 而且由于并发比较多, 捋清楚整个流程也比较难, 此外还有着漫长而折磨的Debug过程. 实在难以详细记录遂放弃. 但是总结下来有以下的核心要点
- 最好先看完整个实验的要求, 在开始, 避免做后面实验的时候对前面改动的地方比较多, 导致改动不全然后艰难的找问题
- 然后, 第一个首先要解决的是计时器的问题. 这个如果弄好, 即使过了3A后面也会有连锁反应; 定时器有很多种实现方案, 可以用time.Timer定时, 每次Reset来重置; 也可以用间隔实现, 没一小段时间计时器苏醒判断是否超时, 每次通过从设开始时间来重置
- 需要善用DPrint, 格式化的日志描述peer间的通信会对Debug有比较多的帮助
- 不要用IDE的table补全, 有时候补全了个似是而非的字段得花很久才能纠正...
- 最后就是一定要捋清楚协议的种种细节, 