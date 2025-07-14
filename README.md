> [课程官网](https://pdos.csail.mit.edu/6.824/schedule.html)
> [推荐译文](https://mit-public-courses-cn-translatio.gitbook.io/mit6-824/lecture-01-introduction)
___
## 概述

MIT6.824(最新的称为6.5840)是关于分布式系统的课程, 有5个基于Go的实验, 每个课程前都有一篇推荐阅读的论文. 整个课程没有很好的中文资源, 难度也不低, 需要花费比较多的时间, 建议保持一个良好的心态耐心的跟完.

此处主要参照了2025 Spring的Schedule. 推荐译文的课程安排较老, 可能存在部分课程顺序不一致, 但总体上内容是差不多的.

由于笔者是目前是大二, 短期内也没有做实验的需求, 大概率也不会去撰写论文, 所以课程中论文相关部分会更侧重于论文的核心思想, 对于实验方法、相关工作等则是能跳过就跳过.
此外, 目标是优先完成Lab, 课程内容Lab中没涉及的则会暂时跳过, 留着后续填坑(应该会填吧🤥)
## Lab
### [Lab1 MapReduce](_docs/Lab1%20MapReduce.md)
### [Lab2 Key Value Server](_docs/Lab2%20Key%20Value%20Server.md)
### [Lab3 Raft](_docs/Lab3%20Raft.md)
### [Lab4 KV Raft](_docs/Lab4%20KV%20Raft.md)
### [Lab5 Sharded KV](_docs/Lab5%20Sharded%20KV.md)
## 课程
### Lecture1 - Introduce

论文阅读: [Paper MapReduce](_docs/Paper%20MapReduce.md)
课程笔记: [Lecture1 Introduce](_docs/Lecture1%20Introduce.md)
### Lecture2 - RPC and Theads

这一节主要关注Go语言的使用, 由于笔者对Go语言使用较多, 故跳过此节. 如果先前没有学习过Go语言可以多花点时间, 课程Schedule页中有配套的在线教程
### Lecture3 Primary-Backup Replication

论文阅读: [[Paper Fault-Tolerant Virtual Machines]]
### Lecture4 Consistency and Linearizablity
### Lecture5,7 Fault Tolearance: Raft

论文阅读: [[Paper Raft]]
### Lecture6 Russ Cox's Lecture

邀请了Russ Cox做讲座, 没有视频也没有教案, 跳过
### Lecture8 GFS
### Lecture9 Zookeeper
### Lecture10 Distributed Transactions
### Lecture11 Lab3A/B Q&A

Q&A链接: [l-raft-QA](https://pdos.csail.mit.edu/6.824/notes/l-raft-QA.txt)
### Lecture12 Spanner
### Lecture13 Optimistic Concurrency Control
### Lecture14 Chardonnay
### Lecture15 Verification of distributed systems
### Lecture16 Cache Consistency: Memcached at Facebook
### Lecture17 Amazon DynamoDB
### Lecture18 AWS Lambda
### Lecture19 Ray
### Lecture20 Fork Consistency
### Lecture21 Peer-to-peer/Bitcoin
### Lecture22 Byzantine Fault Tolerance