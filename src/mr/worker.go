package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// 重试次数
var retries = 5

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % N to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// worker标识
	workerID := "worker-" + strconv.Itoa(rand.Intn(99))

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
			if err := executeMap(workerID, assign, mapf); err != nil {
				fmt.Printf("[%s] Error When Execute %s With Cause: %s\n", workerID, assign.TaskID, err.Error())
			}
			if !finish(workerID, assign.TaskType, assign.TaskID) { // 告知任务处理结束
				return
			}
		case "reduce": // reduce任务
			if err := executeReduce(workerID, assign, reducef); err != nil {
				fmt.Printf("[%s] Error When Execute %s With Cause: %s\n", workerID, assign.TaskID, err.Error())
			}
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

// executeMao 执行map函数
func executeMap(workerID string, assign *AssignTaskReply, mapf func(string, string) []KeyValue) error {
	var err error
	var file *os.File
	var data []byte

	// 读取输入
	if file, err = os.Open(assign.InputFile); err != nil {
		return err
	}
	if data, err = io.ReadAll(file); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
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
			return err
		}
		intermediateFiles = append(intermediateFiles, file)
	}
	// 排序并写入
	for i, f := range intermediateFiles {
		sort.Sort(ByKey(intermediate[i]))
		if data, err = json.Marshal(&intermediate[i]); err != nil {
			return err
		}
		if _, err = f.Write(data); err != nil {
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
	}
	for _, file = range intermediateFiles {
		if err = os.Rename(file.Name(), file.Name()[0:len(file.Name())-4]); err != nil {
			return err
		}
	}
	return nil
}

// executeReduce 执行reduce函数
func executeReduce(workerID string, assign *AssignTaskReply, reducef func(string, []string) string) error {
	var err error
	var data []byte
	var file *os.File
	var kva []KeyValue

	// 读取中间文件
	for i := 0; i < assign.N; i++ {
		// 读取输入
		if file, err = os.Open(strings.Replace(assign.InputFile, "*", strconv.Itoa(i+1), -1)); err != nil {
			return err
		}
		fmt.Printf("[%s] Read Intermediate File: %s\n", workerID, file.Name())
		if data, err = io.ReadAll(file); err != nil {
			return err
		}

		// 解析json
		var kvs []KeyValue
		if err = json.Unmarshal(data, &kvs); err != nil {
			return err
		}
		kva = append(kva, kvs...)

		// 关闭文件
		if err = file.Close(); err != nil {
			return err
		}
	}
	// 排序
	sort.Sort(ByKey(kva))

	if file, err = os.Create(assign.OutputFile); err != nil || file == nil {
		return err
	}
	// 处理相同的key
	for i := 0; i < len(kva); {
		j := i
		var values []string
		for ; j < len(kva) && kva[j].Key == kva[i].Key; j++ {
			values = append(values, kva[j].Value)
		}
		output := reducef(kva[i].Key, values)
		if kva[i].Key == "A" || kva[i].Key == "a" {
			fmt.Printf("[TEST] Reduce Output: %s\n", output)
		}
		if _, err = fmt.Fprintf(file, "%v %v\n", kva[i].Key, output); err != nil {
			return err
		}
		i = j
	}
	fmt.Printf("[Worker] Output In: %s\n", file.Name())
	if err = file.Close(); err != nil {
		return err
	}
	return nil
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
