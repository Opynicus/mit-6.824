package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValues []KeyValue

// sort.Interface implemention three func
func (kv KeyValues) Len() int {
	return len(kv)
}
func (kv KeyValues) Swap(i, j int) {
	kv[i], kv[j] = kv[j], kv[i]
}
func (kv KeyValues) Less(i, j int) bool { // sort by Key
	return kv[i].Key < kv[j].Key
}

type worker struct {
	doing_task bool
	worker_id  int
	mtx        sync.Mutex
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	state := worker{false, -1, sync.Mutex{}}
	for {
		args := getTaskArgs{}
		reply := getTaskReply{}
		success := call("Coordinator.Ask4Task", &args, &reply)

		if !success {
			fmt.Println("Asking 4 Task from Coordinator failed")
			os.Exit(-1)
		}
		// Set Task Id
		state.mtx.Lock()
		state.worker_id = reply.task_id
		state.mtx.Unlock()
		// decide to do map or reduce
		if reply.task_type == MAP { // map task
			doMapTask(mapf, reply.file_name, reply.R_num, state.worker_id)
			msg2Coordinator(MAP, state.worker_id)
		} else if reply.task_type == REDUCE { // reduce task
			doReduceTask(reducef, reply.file_name, reply.M_num, state.worker_id)
			msg2Coordinator(REDUCE, state.worker_id)
		} else {
			time.Sleep(time.Millisecond * 200)
			continue
		}

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

/*
 *  Send msg after task done
 */
func msg2Coordinator(task_type int, task_id int) bool {
	args := postTaskArgs{}
	reply := postTaskReply{}
	args.task_type = task_type
	args.task_id = task_id
	success := call("Coordinator.Sendmsg2Coordinator", &args, &reply)
	return success
}

/*
 * Map Method
 */
func doMapTask(mapf func(string, string) []KeyValue, file_name string, reduce_num int, task_id int) {
	content := mapReadFile(file_name)
	key_values := mapf(file_name, string(content))
	saveIntermediate(key_values, reduce_num, task_id)
}

/*
 * Map worker read file
 */
func mapReadFile(file_name string) []byte {
	file, error := os.Open(file_name)
	if error != nil {
		log.Fatalf("Can't Open %v", file_name)
	}
	content, error := ioutil.ReadAll(file)
	if error != nil {
		log.Fatalf("Can't Read %v", file_name)
	}
	file.Close()
	return content
}

/*
 * Map worker divided file into 'reduce_num' part for reduce worker to deal with
 */
func saveIntermediate(key_values KeyValues, reduce_num int, task_id int) {
	intermediate := KeyValues{}
	intermediate = append(intermediate, key_values...)
	distributed_files := make([]*os.File, reduce_num) // to store files after partition
	encode_files := make([]*json.Encoder, reduce_num)
	// make partition
	for i := 0; i < reduce_num; i++ {
		file, err := ioutil.TempFile(".", "m-out") //TempFile and atomic rename trick
		if err != nil {
			log.Fatalf("create temp file failed")
		}
		enc := json.NewEncoder(file)

		distributed_files[i] = file
		encode_files[i] = enc
	}
	// encode
	for _, key_value := range intermediate {
		p := ihash(key_value.Key) % reduce_num
		err := encode_files[p].Encode(&key_value)
		if err != nil {
			log.Fatalf("encode key-value pair %v %v failed", key_value.Key, key_value.Value)
		}
	}

	for i := 0; i < reduce_num; i++ {
		os.Rename(distributed_files[i].Name(), fmt.Sprintf("mr-%v-%v", task_id, i))
		distributed_files[i].Close()
	}
}

/*
 * initializing reduce worker
 * Reduce worker get intermediate
 */
func readIntermediate(intermediate KeyValues, map_num int, task_id int) KeyValues {
	for i := 0; i < map_num; i++ {
		file, error1 := os.Open(fmt.Sprintf("mr-%v-%v", i, task_id))
		if error1 != nil {
			log.Fatalf("can't read file %v", fmt.Sprintf("mr-%v-%v", i, task_id))
		}

		dec := json.NewDecoder(file)
		for {
			var key_value KeyValue
			error2 := dec.Decode(&key_value)
			if error2 != nil {
				log.Fatalf("Decode Failed")
				break
			}
			intermediate = append(intermediate, key_value)
		}
	}
	return intermediate
}

/*
 * Reduce Method
 */
func doReduceTask(reducef func(string, []string) string, file_name string, map_num int, task_id int) {
	intermediate := KeyValues{}
	intermediate = readIntermediate(intermediate, map_num, task_id) // initializing intermediate
	sort.Sort(KeyValues(intermediate))                              // make key-value pairs consecutive
	create_file_name := fmt.Sprintf("mr-out-%v", task_id)
	file, _ := os.Create(create_file_name)

	// Find consecutive key-value pairs and store them in 'values', calculate nums, then output to reducef
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	file.Close()
}
