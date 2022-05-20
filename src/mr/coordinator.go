package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type DelivrMapTask struct {
	is_done            bool
	file_name          string
	state              int
	last_map_task_time time.Time
}

type DelivrReduceTask struct {
	is_done               bool
	state                 int
	last_reduce_task_time time.Time
}

type Coordinator struct {
	// Your definitions here.
	map_num    int
	reduce_num int

	delivr_map_task    []DelivrMapTask
	delivr_reduce_task []DelivrReduceTask

	map_task_done    int
	reduce_task_done int
	all_done         bool

	mtx sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) timeCheck() {
	for !c.all_done {
		c.mtx.Lock()
		for i := 0; i < c.map_num; i++ {
			if c.delivr_map_task[i].state == RUNNING && time.Now().Sub(c.delivr_map_task[i].last_map_task_time) > time.Second*10 {
				c.delivr_map_task[i].state = TINEOUT
			}
		}
		for i := 0; i < c.reduce_num; i++ {
			if c.delivr_reduce_task[i].state == RUNNING && time.Now().Sub(c.delivr_reduce_task[i].last_reduce_task_time) > time.Second*10 {
				c.delivr_reduce_task[i].state = TINEOUT
			}
		}
		c.mtx.Unlock()
		time.Sleep(time.Second * 5)
	}
}

func (c *Coordinator) AskTask(args *getTaskArgs, reply *getTaskReply) error {
	c.mtx.Lock()

	for i := 0; i < c.map_num; i++ {
		if c.delivr_map_task[i].state == WAIT {
			reply.task_type = MAP
			reply.task_id = i
			reply.file_name = c.delivr_map_task[i].file_name
			reply.M_num = c.map_num
			reply.R_num = c.reduce_num
			c.delivr_map_task[i].state = RUNNING
			c.delivr_map_task[i].last_map_task_time = time.Now()
			c.mtx.Unlock()
			return nil
		} else if c.delivr_map_task[i].state == TINEOUT {
			// log.Printf("TIMEOUT map task delivr", c.delivr_map_task[i])
			reply.task_type = MAP
			reply.task_id = i
			reply.file_name = c.delivr_map_task[i].file_name
			reply.M_num = c.map_num
			reply.R_num = c.reduce_num
			c.delivr_map_task[i].state = RUNNING
			c.delivr_map_task[i].last_map_task_time = time.Now()
			c.mtx.Unlock()
			return nil
		}
	}

	if c.map_task_done < c.map_num {
		reply.task_type = NONE
		c.mtx.Unlock()
		return nil
	}

	for i := 0; i < c.reduce_num; i++ {
		if c.delivr_reduce_task[i].state == WAIT {
			reply.task_type = REDUCE
			reply.task_id = i
			reply.file_name = ""
			reply.M_num = c.map_num
			reply.R_num = c.reduce_num
			c.delivr_reduce_task[i].state = RUNNING
			c.delivr_reduce_task[i].last_reduce_task_time = time.Now()
			c.mtx.Unlock()
			return nil
		} else if c.delivr_reduce_task[i].state == TINEOUT {
			// log.Printf("TIMEOUT map task delivr", c.delivr_reduce_task[i])
			reply.task_type = REDUCE
			reply.task_id = i
			reply.file_name = ""
			reply.M_num = c.map_num
			reply.R_num = c.reduce_num
			c.delivr_reduce_task[i].state = RUNNING
			c.delivr_reduce_task[i].last_reduce_task_time = time.Now()
			c.mtx.Unlock()
			return nil
		}

	}

	reply.task_type = NONE
	c.mtx.Unlock()
	return nil
}

func (c *Coordinator) askMsg(args *postTaskArgs, reply *postTaskReply) error {
	c.mtx.Lock()
	if args.task_type == MAP {
		if c.delivr_map_task[args.task_id].state != DONE {
			c.map_task_done++
			c.delivr_map_task[args.task_id].state = DONE
		}
	} else if args.task_type == REDUCE {
		if c.delivr_reduce_task[args.task_id].state != DONE {
			c.reduce_task_done++
			c.delivr_reduce_task[args.task_id].state = DONE
		}
	}

	if c.isMapTaskDone() && c.isReduceTaskDone() {
		c.all_done = true
	}

	c.mtx.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	c.mtx.Lock()
	ret = c.all_done
	c.mtx.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.reduce_num = nReduce
	c.map_num = len(files)
	c.delivr_map_task = make([]DelivrMapTask, c.map_num)
	c.delivr_reduce_task = make([]DelivrReduceTask, c.reduce_num)
	c.all_done = false
	c.map_task_done = 0
	c.reduce_task_done = 0
	c.mtx = sync.Mutex{}

	for i, f := range files {
		c.delivr_map_task[i].file_name = f
		c.delivr_map_task[i].is_done = false
		c.delivr_map_task[i].state = WAIT
	}

	for i := 0; i < nReduce; i++ {
		c.delivr_reduce_task[i].is_done = false
		c.delivr_reduce_task[i].state = WAIT
	}

	go c.timeCheck()
	c.server()
	return &c
}

func (c *Coordinator) isReduceTaskDone() bool {
	is_done := false
	c.mtx.Lock()
	if c.reduce_task_done >= c.reduce_num {
		is_done = true
	}
	c.mtx.Unlock()
	return is_done
}

func (c *Coordinator) isMapTaskDone() bool {
	is_done := false
	c.mtx.Lock()
	if c.map_task_done >= c.map_num {
		is_done = true
	}
	c.mtx.Unlock()
	return is_done
}
