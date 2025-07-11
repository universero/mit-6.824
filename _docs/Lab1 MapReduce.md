___
## Lab文档
### Introduce

在这个实验中, 我们需要构建一个MapReduce System. 我们需要实现worker, 调用Map和Reduce函数并且处理文件的读写, 以及实现master(coordinator), 负责任务的分发和错误worker的处理.

### Getting Started

需要Go 1.22或更高的版本
最好和文档中保持一致使用Linux环境, 如果是windows系统, 可以正常开发, 编译在wsl中完成
通过如下命令获取初始的lab仓库
```sh
git clone git://g.csail.mit.edu/6.5840-golabs-2025 6.5840
```
在`src/main/mrsequential.go`中提供了一个简单的顺序MapReduce实现, 单个进程中每次执行一个map或reduce任务. 还提供了一对MapReduce应用: mrapps/wc.go 统计单词个数, `mraaps/indexer.go`文本索引. 可以用如下方式执行顺序的word count
```sh
cd src/main
go build -buildmode=plugin ../mrapps/wc.go // 指定构建模式为Go插件, 会生成so或ddl, 允许主程序运行时动态加载
rm mr-out*
go run mrsequential.go wc.so pg*.txt
```
mrsequential.go中文件会输出到mr-out-0, 输入文件来源于pg-xxx.txt的文本文件
可以自由的从mrsequnetial.go中"借"代码, 也能看看mrapps/wc.go学习MapReduce application的代码如何编写

### Your Job

任务是实现一个分布式的MapReduce, 包括两个程序, coordinator(master)和worker. 会有一个coordinator, 一个或多个worker并行执行. 在实际的系统中worker会在许多不同的机器上执行, 但是在实验中只需要在一台机器上跑就行. workers需要通过RPC来和coordinator交互, 每个worker进程会向coordinator循环地请求任务, 读取任务的输入从一个或多个文件, 执行任务, 将任务的输出写入一个或多个文件. coordinator需要在一个worker没有在给定时间(这个lab中是10s)内完成任务时, 将相同的任务派发给其他的worker

`main/mrcoordinator.go`和`main/mrworker.go`不应该被修改, 实现应该完成在`mr/coordinator.go`,`mr/woker.go`和`mr/rpc.go`中

下面是如何在你的代码中执行wc
```sh
// 首先, 和前面一样编译wc.go
go build -buildmode=plugin ../mrapps/wc.go
// 在main文件夹中执行coordinator, 每个txt对应一个split也是一个map任务的输入
rm mr-out*
go run mrcoordinator.go pg-*.txt
// 在更多的窗口中执行worker
go run mrworker.go wc.so
```

`main/test-mr.sh`可以校验wc和indexer是否正确执行, 同时也会检测你的实现中map和reduce是否并行执行, 以及能否从worker的crash中恢复
如果现在直接执行test脚本会一直挂着不退出, 因为coordinator没有结束. 可以将`mr/coordinator.go`中的ret := false改为true

你可能看到Go RPC包中的一些错误, 如
```sh
2019/12/16 13:27:09 rpc.Register: method "Done" has 1 input parameters; needs exactly three
```
忽略这些信息, 将coordinator注册为RPC server检测其所有方法是否适合RPCs(有三个输入), Done不通过RPC调用
此外根据终止worker的策略可能也会看到如下的错误
2025/02/11 16:21:32 dialing:dial unix /var/tmp/5840-mr-501: connect: connection refused
每次测试看到少量这样的消息是正常的, 当coordinator退出后, worker无法连接到RPC server就会出现这样的消息
### A few rules

- Map阶段应该将中间key划分到桶中, 用于`nReduce`的reduce task, nReduce是reduce tasks的数量, 由`main/mrcoordinator.go`传递给`MakeCoordinator()`. 每个map应该创建nReduce个中间文件共reduce任务消费
- worker应该实现将第X个reduce的结果输出到mr-out-X
- mr-out-X文件中, 每个Reduce function的输出占一行, 由`"%v %v"`的format产生, 前面是key后面是value, 可以在`main/mrsequential.go`中查找"this is the correct format"注释的那一行
- worker应该将中间map输出到正确的文件目录下, 以便后续reduce的输入
- `main/mrcoordinator.go`期望`mr/coordinator.go`实现一个`Done()`方法, 当MapReduce 工作全部完成时返回true, 此时`mrcoordinator.go`将退出
- 当工作全部完成后, worker也应该退出. 一个简单的实现方式是使用call()的返回值: 如果worker无法连接到coordinator, 可以假定工作已经完成. 根据你的设计, 你可能还会发现, 设置一个给worker的伪任务可能会很有帮助.
### Hints

- 一种开始的方式是修改`mr/worker.go`的`Worker()`从而实现coordinator发送RPC请求任务. 然后修改coordinator, 使其以尚未启动的Map任务的文件名作为响应. 然后修改worker, 使其读取该文件并调用map, 如`mrsequential.go`所示
- Map和Reduce在运行时从.so中加载
- 如果更改了mr/目录下的内容, 可能需要重新构建MapReduce插件
- 中间文件合理的命名时mr-X-Y, 其中X是Map任务编号, Y是Reduce任务编号
- worker的map任务代码需要一种方式将中间kv对存储到文件中且需要被reduce正确的读取, 一种可能的方式是使用`encoding/json`, 将中间kv对以json形式存储
- 可以使用worker.go中的`ihash(key)`方法来从给定的task中选择reduce任务
- 可以从`mrsequential.go`中借用读取输入文件, 排序中间kv对和排序输出的代码
- 使用`go run -race` 判断是否有竟态条件
- worker可能需要等待, 比如reduces在最后一个map完成前不能开始, 一种可能的实现是worker周期性的向coordinator请求, 并在两次请求间`time.sleep()`, 另一种肯的实现是coordinator中相关的RPC handler有一个循环等待, 使用time.Sleep或sync.Cond. Go的每个RPC handler运行在独立的线程, 所以一个handler的等待不会组织coordinator处理其他的RPC
- coordinator无法可靠的区分crashed的worker, 仍在运行但因某种原因停滞的worker和正在执行任务但速度太慢的worker. 最好的是让coordinator等待一段时间(本实验10s)然后认为该worker已死亡, 然后讲任务重新分配给其他的worker
- 如果你选择实现Backup Tasks, 由于测试中会判断worker正常执行时会不会分配冗余任务, 所以Backup tasks应该在一个相对较长的时间后分配
- 如果要测试crash recovery, 可以使用crash.go插件, 它随机的存在map和reduce函数中
- 为了确保没有人在出现崩溃的情况下观察到部分写入的文件，MapReduce论文提到了使用临时文件并在完成写入后自动重命名它的技巧。你可以用ioutil。TempFile(或os.CreateTemp（如果你正在运行Go 1.17或更高版本）来创建临时文件和os.Rename来原子地重命名
- test-mr.sh在目录mr-tmp下运行, 可以在其中查看中间文件
- test-mr-many.sh会多次测试以发现低概率错误, 将测试次数作为参数
- RPC只会发送导出的字段. 调用RPC call()的适合, reply struct应该只包含默认值
## 过程记录
### 任务梳理

修改mr文件夹下的coordinator.go, rpc.go和worker.go三个文件, 并通过test-mr.sh的测试
通信方式: RPC
coordinator
- 机制
	- 任务管理: 管理各任务的状态
	- 任务分配: 向可用的worker分派任务
	- 任务重派: worker在10s后没有完成任务则重新派发
worker
- 机制
	- 任务请求: 向coordinator请求任务, 闲时time.sleep等待
	- 函数加载: 从Map和Reduce中加载.so
	- 执行函数: 使用ihash(key)选择合适的reduce任务
	- 中间文件生成: 生成mr-X-Y的中间文件, json格式, os.CreateTemp和os.Rename来保证中间文件的生成的原子性
### mrsequential代码分析

流程梳理
- 从输入中解析map函数和reduce函数
- 从输入文件中读取读入, 用map函数处理, 添加到intermediate中(这里和实际MapReduce不同, 没有拆分到NxM个桶里而是存在一处)
- 将intermediate排序
- 在每个不同的key上执行reduce函数然后输出到文件中
### 任务管理

#### RPC请求结构
根据hints, 首先完成RPC请求部分. worker执行任务需要的参数有
- map: 输入文件名
- reduce: 作为输入的中间键值对, 输出文件名
此外, 执行结束后还需要告知coordinator, 需要给到的参数有
- workerID: workerID
- taskID: 任务ID
coordinator还需要存储每个任务的状态和正在执行的节点的标识
综上可以分析出如下的RPC请求响应结构
```go
// AssignTaskRequest 任务请求  
type AssignTaskRequest struct {  
    WorkerID string // worker标识  
}  
  
// AssignTaskReply 任务响应  
type AssignTaskReply struct {  
    TaskType   string // 任务类型, map, reduce, exit(job完成通知worker退出)  
    TaskID     string // 任务ID  
	N          int    // 任务数量, Map任务得知的是Reduce的数量, 反之亦然
    InputFile  string // 输入文件位置  
    OutputFile string // 输出文件位置  
}

// FinishTaskRequest 任务完成请求  
type FinishTaskRequest struct {  
    WorkerID string // worker标识
    TaskType string // 任务类型
    TaskID   string // 任务标识  
}  
  
// FinishTaskReply 任务完成响应  
type FinishTaskReply struct {  
    Received bool // 是否正确接收  
}
```
#### worker请求部分

暂时先不考虑具体任务的执行, 可以写出任务分配请求和完成请求
```go
// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// worker标识
	workerID := "worker-" + strconv.Itoa(rand.Intn(10))

	for {
		// 请求任务
		req := &AssignTaskRequest{WorkerID: workerID}
		assign := &AssignTaskReply{}
		if !retryCall(workerID, "Coordinator.Assign", req, assign) {
			fmt.Printf("[%s] Exit\n", workerID)
			return // 任务获取失败, 认为coordinator宕机, 退出
		}
		fmt.Printf("[%s] Recived A Task As Follow\n%s", workerID, assign.String())

		// 处理任务
		switch assign.TaskType {
		case "map": // map 任务
			// TODO 处理map
			if !finish(workerID, assign.TaskType, assign.TaskID) { // 告知任务处理结束
				return
			}
		case "reduce": // reduce任务
			// TODO 处理 reduce
			if !finish(workerID, assign.TaskType, assign.TaskID) { // 告知任务处理结束
				return
			}
		case "wait": // 暂无任务
			time.Sleep(1 * time.Second)
		case "exit": // job完成, 退出任务
			fmt.Printf("[%s] Exit\n", workerID)
			return
		}
	}
}

func finish(workerID, taskType, taskID string) bool {
	req := &FinishTaskRequest{WorkerID: workerID, TaskType: taskType, TaskID: taskID}
	reply := &FinishTaskReply{}
	return retryCall(workerID, "Coordinator.Finish", req, reply) && reply.Received
}

func retryCall(workerID, rpcname string, args interface{}, reply interface{}) bool {
	for i := 0; !call(rpcname, args, reply); i++ {
		fmt.Printf("[%s] RPC Call %s Error; has tried %d times\n", workerID, rpcname, i+1)
		if i >= retries {
			fmt.Printf("[%s] RPC Call %s Error; too many times and will exit \n", workerID, rpcname)
			return false
		}
	}
	return true
}
```
为了保证可靠性, 在call的基础上封装了一个retryCall来实现重试, 如果重试五次都没有获取到则退出Worker. 然后不实际处理任务, 直接全部退出
#### coordinator响应部分

忽略实际任务分配部分, 直接响应任务是否完成. 用一个done字段模拟任务完成
```go
type Coordinator struct {
	done bool
}

// Assign 处理任务请求  
func (c *Coordinator) Assign(args *AssignTaskRequest, reply *AssignTaskReply) error {  
    // TODO 分配实际任务  
    fmt.Printf("%-13s%-30s%s\n", "[coordinator]", "Received assign request from", args.WorkerID)  
    if c.done {  
       reply.TaskType = "exit"  
       reply.TaskID = "exit-task"  
    } else {  
       reply.TaskType = "map"  
       reply.TaskID = "test-task"  
    }  
  
    return nil  
}  
  
// Finish 处理任务完成  
func (c *Coordinator) Finish(args *FinishTaskRequest, reply *FinishTaskReply) error {  
    fmt.Printf("%-13s%-30s%s\n", "[coordinator]", "Received finish request from", args.WorkerID)  
    reply.Received = true  
    c.done = true  
    return nil  
}
```
由于只有一个任务所以在finish中标识c.done为true.
完成到这里, 通过手动执行coordinator和worker可以发现, 目前已经可以正确的处理的两者直接的通信
#### coordinator输入处理

首先, 对于map任务, 我们要存储原始输入文件的位置, 这个Map文件对应的状态, 被分配的worker, 中间文件应该输出的位置. Reduce同理
所以我们可以在Coordinator中维护如下用于管理任务的结构体
```go
type Task struct {  
    ID         string   // 顺序编号  
    InputFile  string   // 原始输入文件  
    OutputFile string   // 中间文件应该输出的位置  
    State      string   // 状态  
    Worker     []string // 被分配到的worker, 考虑backup tasks, 一个任务可能同时在多个节点上执行  
}  
  
type TaskManager struct {  
    Idle       map[string]*Task // 空闲的任务  
    InProgress map[string]*Task // 进行中任务  
    Complete   map[string]*Task // 完成的任务
}  
  
type Coordinator struct {  
    phase string // 管理阶段, map, reduce, done  
    // Map任务数  
    NMap int  
    Map  *TaskManager  
    // Reduce任务数  
    NReduce int  
    Reduce  *TaskManager  
}
```
有了上述的定义后就可以开始初始化Coordinator
```go
// initialise 处理输入并初始化任务  
func (c *Coordinator) initialise(files []string, nReduce int) {  
    c.NMap, c.NReduce = len(files), nReduce  
    c.Map.Idle, c.Map.InProgress, c.Map.Complete = make(map[string]*Task), make(map[string]*Task), make(map[string]*Task)  
    c.Reduce.Idle, c.Reduce.InProgress, c.Reduce.Complete = make(map[string]*Task), make(map[string]*Task), make(map[string]*Task)  
    // init map task  
    for i, f := range files {  
       m := &Task{  
          ID:         "MapTask-" + strconv.Itoa(i+1),  
          InputFile:  f,  
          OutputFile: "mr-" + strconv.Itoa(i+1) + "-*", // 不同Reduce输出位置不同, 这里用*作为通配符  
          State:      "idle",  
          Worker:     []string{},  
       }  
       c.Map.Idle[m.ID] = m  
    }  
    fmt.Printf("Map Task Initialisation Finished;\n")  
  
    // init reduce task  
    for i := range nReduce {  
       r := &Task{  
          ID:         "ReduceTask-" + strconv.Itoa(i+1),  
          InputFile:  "mr-*-" + strconv.Itoa(i+1),  
          OutputFile: "mr-out-" + strconv.Itoa(i+1),  
          State:      "idle",  
          Worker:     []string{},  
       }  
       c.Reduce.Idle[r.ID] = r  
    }  
    fmt.Printf("Reduce Task Initialisation Finished;\n")  
  
    // 进入map阶段  
    c.phase = "map"  
    fmt.Printf("Coordinator State As Follow\n %s", c.String())  
}
```

此外还让AI实现了一个美观的Coordinator状态查看的方法
```go
func (c *Coordinator) String() string {  
    var sb strings.Builder  
  
    // 头部信息  
    sb.WriteString("Map Task Initialisation Finished\n")  
    sb.WriteString("Coordinator State:\n")  
    sb.WriteString("================================\n")  
  
    // Map 任务状态  
    sb.WriteString("[Map Tasks]\n")  
    sb.WriteString(fmt.Sprintf("  Idle:       %d tasks\n", len(c.Map.Idle)))  
    sb.WriteString(fmt.Sprintf("  InProgress: %d tasks\n", len(c.Map.InProgress)))  
    sb.WriteString(fmt.Sprintf("  Complete:   %d tasks\n", len(c.Map.Complete)))  
  
    // Reduce 任务状态  
    sb.WriteString("\n[Reduce Tasks]\n")  
    sb.WriteString(fmt.Sprintf("  Idle:       %d tasks\n", len(c.Reduce.Idle)))  
    sb.WriteString(fmt.Sprintf("  InProgress: %d tasks\n", len(c.Reduce.InProgress)))  
    sb.WriteString(fmt.Sprintf("  Complete:   %d tasks\n", len(c.Reduce.Complete)))  
  
    // 详细信息  
    sb.WriteString("\n[Details]\n")  
    sb.WriteString("Map Tasks:\n")  
    for id, task := range c.Map.Idle {  
       sb.WriteString(fmt.Sprintf("  %-9s: %-23s [State: %s, Workers: %v]\n",  
          id, task.InputFile, task.State, task.Worker))  
    }  
    for id, task := range c.Map.InProgress {  
       sb.WriteString(fmt.Sprintf("  %-9s: %-23s [State: %s, Workers: %v]\n",  
          id, task.InputFile, task.State, task.Worker))  
    }  
    for id, task := range c.Map.Complete {  
       sb.WriteString(fmt.Sprintf("  %-9s: %-23s [State: %s, Workers: %v]\n",  
          id, task.InputFile, task.State, task.Worker))  
    }  
  
    sb.WriteString("\nReduce Tasks:\n")  
    for id, task := range c.Reduce.Idle {  
       sb.WriteString(fmt.Sprintf("  %-13s: %-9s [State: %s, Workers: %v]\n",  
          id, task.InputFile, task.State, task.Worker))  
    }  
    for id, task := range c.Reduce.InProgress {  
       sb.WriteString(fmt.Sprintf("  %-13s: %-9s [State: %s, Workers: %v]\n",  
          id, task.InputFile, task.State, task.Worker))  
    }  
    for id, task := range c.Reduce.Complete {  
       sb.WriteString(fmt.Sprintf("  %-13s: %-9s [State: %s, Workers: %v]\n",  
          id, task.InputFile, task.State, task.Worker))  
    }  
  
    return sb.String()  
}
```
#### coordinator任务分配

任务分配会有以下这些情况
- job已完成, 通知请求的worker退出
- 没有可分配的任务, 通知请求的worker等待
- 分配一个map / reduce 任务
根据这些case可以完成任务分配的方法
```go
// Assign 处理任务请求  
func (c *Coordinator) Assign(args *AssignTaskRequest, reply *AssignTaskReply) error {  
    fmt.Printf("%-14s%-30s%s\n", "[coordinator]", "Received assign request from", args.WorkerID)  
    var todo *TaskManager  
  
    switch c.phase {  
    case "done": // job完成, 通知worker退出  
       reply.TaskType = "exit"  
       reply.TaskID = "everything was done, please exit"  
       return nil  
    case "map":  
       reply.TaskType = "map"  
       reply.N = c.NReduce  
       todo = c.Map  
    case "reduce":  
       reply.TaskType = "reduce"  
	   reply.N = c.NMap  
       todo = c.Reduce  
    default:  
       return errors.New("[coordinator] unknow phase: " + c.phase)  
    }  
  
    if len(todo.Idle) == 0 { // 无可分配任务  
       reply.TaskType = "wait"  
       reply.TaskID = "nothing to do, please wait"  
       return nil  
    }  
  
	// 分配任务, 并移动至进行中队列 TODO 超时重派
	var k string
	var v *Task
	for key, val := range todo.Idle {
		k, v = key, val
		break
	}

	v.State, v.Worker = "in-progress", append(v.Worker, args.WorkerID)
	todo.InProgress[k] = v
	delete(todo.Idle, k)
	reply.TaskID, reply.InputFile, reply.OutputFile = v.ID, v.InputFile, v.OutputFile
	go c.monitor(todo, v.ID, args.WorkerID)
	return nil
}
```
这里还缺少的是对每个进行中任务进行限时, 10s未完成则视作worker出错, 重新分配, 这个留到整体能正确执行后再完善
#### coordinator任务完成

当任务后, coordinator需要将其从in-progress状态切换为done的状态, 逻辑很简单, 只需要注意coordinator的状态流转就行
```go
// Finish 处理任务完成
func (c *Coordinator) Finish(args *FinishTaskRequest, reply *FinishTaskReply) error {
	fmt.Printf("%-14s%-30s%s\n", "[coordinator]", "Received finish request from", args.WorkerID)
	reply.Received = true

	var todo *TaskManager
	switch args.TaskType {
	case "map":
		todo = c.Map
	case "reduce":
		todo = c.Reduce
	default:
		return errors.New("[coordinator] unknow task type: " + args.TaskType)
	}

	// 任务完成
	if task, ok := todo.InProgress[args.TaskID]; ok {
		task.State, task.Worker = "done", []string{args.WorkerID}
		todo.Complete[task.ID] = task
		delete(todo.InProgress, task.ID)
	}
	// 状态流转
	if args.TaskType == "map" && len(todo.Complete) == c.NMap {
		c.phase = "reduce"
	} else if args.TaskType == "reduce" && len(todo.Complete) == c.NReduce {
		c.phase = "done"
	}
	return nil
}
```

此时Done也可以完成了, 当Reduce任务全部完成即是整个MapReduce Job完成.
```go
func (c *Coordinator) Done() (ret bool) {
	if len(c.Reduce.Complete) == c.NReduce {
		fmt.Printf("[coordinator] MapReduce Job Is Done\n\n")
		ret, c.phase = true, "done"
		time.Sleep(5 * time.Second) // 等待worker结束
	}
	return ret
}
```
### 任务执行
#### Map任务

map任务的执行参考mrsequential里的代码可以发现过程非常的简单, 读入文件内容, 用map函数处理一次, 将中间结果根据Key排序, 写入到中间文件中, 只需要稍微细化一下细节即可.

```go
// executeMao 执行map函数
func executeMap(workerID string, assign *AssignTaskReply, mapf func(string, string) []KeyValue) bool {
	var err error
	var file *os.File
	var data []byte

	// 读取输入
	if file, err = os.Open(assign.InputFile); err != nil {
		return false
	}
	if data, err = io.ReadAll(file); err != nil {
		return false
	}
	if err = file.Close(); err != nil {
		return false
	}

	// map函数处理
	kva := mapf(assign.InputFile, string(data))

	// 按hash分组
	var intermediate = make([][]KeyValue, assign.N)
	for _, kv := range kva {
		intermediate[ihash(kv.Key)%assign.N] = append(intermediate[ihash(kv.Key)%assign.N], kv)
	}

	// 打开一批中间文件
	var intermediateFiles []*os.File
	for i := 0; i < assign.N; i++ {
		file, err = os.Create(strings.Replace(assign.OutputFile, "*", strconv.Itoa(i+1), -1) + "-tmp")
		if err != nil {
			return false
		}
		intermediateFiles = append(intermediateFiles, file)
	}
	// 排序并写入
	for i, f := range intermediateFiles {
		sort.Sort(ByKey(intermediate[i]))
		if data, err = json.Marshal(&intermediate[i]); err != nil {
			return false
		}
		if _, err = f.Write(data); err != nil {
			return false
		}
	}
	for _, file = range intermediateFiles {
		if err = os.Rename(file.Name(), file.Name()[0:len(file.Name())-4]); err != nil {
			return false
		}
	}
	return true
}
```
#### Reduce任务

reduce需要该任务对应的所有中间文件中读取输入, 排序, 用reducef处理后输出到对应的out文件中
```go
// executeReduce 执行reduce函数
func executeReduce(workerID string, assign *AssignTaskReply, reducef func(string, []string) string) bool {
	var err error
	var data []byte
	var file *os.File
	var kva []KeyValue

	// 读取中间文件
	for i := 0; i < assign.N; i++ {
		// 读取输入
		if file, err = os.Open(strings.Replace(assign.InputFile, "*", strconv.Itoa(i+1), -1)); err != nil {
			return false
		}
		fmt.Printf("[%s] Read Intermediate File: %s\n", workerID, file.Name())
		if data, err = io.ReadAll(file); err != nil {
			return false
		}

		// 解析json
		var kvs []KeyValue
		if err = json.Unmarshal(data, &kvs); err != nil {
			return false
		}
		kva = append(kva, kvs...)

		// 关闭文件
		if err = file.Close(); err != nil {
			return false
		}
	}
	// 排序
	sort.Sort(ByKey(kva))

	if file, err = os.Create(assign.OutputFile); err != nil {
		return false
	}
	// 处理相同的key
	for i := 0; i < len(kva); {
		j := i
		var values []string
		for ; j < len(kva) && kva[j].Key == kva[i].Key; j++ {
			values = append(values, kva[j].Value)
		}
		output := reducef(kva[i].Key, values)
		if _, err = fmt.Fprintf(file, "%v %v\n", kva[i].Key, output); err != nil {
			return false
		}
		i = j
	}
	if err = file.Close(); err != nil {
		return false
	}
	return true
}
```
### 超时任务

首先, 我们需要在TaskManager中增加一个字段用于检索所有的任务, 在初始化时所有类型的任务都需要加进去
```go
type Coordinator struct {
	phase string           // 管理阶段, map, reduce, done
	Tasks map[string]*Task // 所有任务
	// Map任务数
	NMap int
	Map  *TaskManager
	// Reduce任务数
	NReduce int
	Reduce  *TaskManager
}
```

为了能判断每个任务是否超时, 每次分配一个任务时, 都需要有一个routine来监听它. 当超时十秒, 如果该任务还没完成, 就将这个任务修改为空闲, 然后等待重新分配这个任务.
监听的代码如下
```go
// monitor 任务超时则重置为idle
func (c *Coordinator) monitor(todo *TaskManager, taskID string, workerID string) {
	clock := time.After(expire)
	select {
	case <-clock:
		c.mu.Lock()
		defer c.mu.Unlock()
		if t := c.Tasks[taskID]; t.State == "in-progress" {
			fmt.Printf("[coordinator] Task %s Is Expire On %s\n", taskID, workerID)
			var workers []string
			for _, w := range t.Worker {
				if w != workerID {
					workers = append(workers, w)
				}
			}
			t.State, t.Worker = "idle", workers
			delete(todo.InProgress, taskID)
			todo.Idle[taskID] = t
		}
		return
	}
}
```

每次分配后都需要启动一个goroutine执行这个方法, 此外为了避免竞态条件, 需要给Coordinator加上一个mutex, 在monitor, Assign和Finish中操作Tasks, Map和Reduce时都需要加锁
### 总结

上述内容其实思路都满直接的, 只要按照流程循序渐进即可
有个卡的比较久的bug是,  executeMap时没有关闭写入的中间文件, 导致reduce无法读取