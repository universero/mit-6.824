package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strings"
)
import "strconv"

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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func (r *AssignTaskReply) String() string {
	var sb strings.Builder

	// 头部信息
	sb.WriteString("Task Assignment Details:\n")
	sb.WriteString("=======================\n")

	// 对齐格式：字段名占20字符，值左对齐
	format := "  %-20s: %-v\n"

	// 输出各字段
	sb.WriteString(fmt.Sprintf(format, "Task Type", r.TaskType))
	sb.WriteString(fmt.Sprintf(format, "Task ID", r.TaskID))
	sb.WriteString(fmt.Sprintf(format, "Reduce Tasks Count", r.N))
	sb.WriteString(fmt.Sprintf(format, "Input File", r.InputFile))
	sb.WriteString(fmt.Sprintf(format, "Output File", r.OutputFile))
	sb.WriteString("\n")
	return sb.String()
}
