package mr

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import (
	"net"
	"net/http"
	"net/rpc"
	"os"
)

var expire = 10 * time.Second

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
	phase string           // 管理阶段, map, reduce, done
	Tasks map[string]*Task // 所有任务
	mu    sync.Mutex

	// Map任务数
	NMap int
	Map  *TaskManager
	// Reduce任务数
	NReduce int
	Reduce  *TaskManager
}

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

	c.mu.Lock()
	defer c.mu.Unlock()
	v.State, v.Worker = "in-progress", append(v.Worker, args.WorkerID)
	todo.InProgress[k] = v
	delete(todo.Idle, k)
	go c.monitor(todo, v.ID, args.WorkerID)

	reply.TaskID, reply.InputFile, reply.OutputFile = v.ID, v.InputFile, v.OutputFile
	return nil
}

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

	c.mu.Lock()
	defer c.mu.Unlock()
	if task, ok := todo.InProgress[args.TaskID]; ok {
		task.State, task.Worker = "done", []string{args.WorkerID}
		todo.Complete[task.ID] = task
		delete(todo.InProgress, task.ID)
	}

	// 状态流转
	if c.phase == "map" && len(todo.Complete) == c.NMap {
		c.phase = "reduce"
	} else if c.phase == "reduce" && len(todo.Complete) == c.NReduce {
		c.phase = "done"
	}
	return nil
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() (ret bool) {
	if len(c.Reduce.Complete) == c.NReduce {
		fmt.Printf("[coordinator] MapReduce Job Is Done\n\n")
		ret, c.phase = true, "done"
		time.Sleep(5 * time.Second) // 等待worker结束
	}
	return ret
}

// MakeCoordinator create a Coordinator.
// main/coordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{Map: &TaskManager{}, Reduce: &TaskManager{}}
	c.initialise(files, nReduce)
	c.server()
	return &c
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// initialise 处理输入并初始化任务
func (c *Coordinator) initialise(files []string, nReduce int) {
	c.NMap, c.NReduce, c.Tasks = len(files), nReduce, make(map[string]*Task)
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
		c.Tasks[m.ID] = m
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
		c.Tasks[r.ID] = r
	}
	fmt.Printf("Reduce Task Initialisation Finished;\n")

	// 进入map阶段
	c.phase = "map"
	fmt.Printf("Coordinator State As Follow\n %s", c.String())
}

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
