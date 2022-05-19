package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

const (
	MAP    = 0
	REDUCE = 1
)

type getTaskArgs struct {
	worker_id int
}

type getTaskReply struct {
	task_type int
	task_id   int
	M_num     int
	R_num     int
	file_name string
}

type postTaskArgs struct {
	task_type int
	task_id   int
}

type postTaskReply struct {
	success bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
