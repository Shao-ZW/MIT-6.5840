package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io"
import "strconv"
import "sort"
import "encoding/json"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		args := Args{}
		reply := Reply{}
		ok := call("Coordinator.AssignTaskHandler", &args, &reply)

		if !ok || reply.TaskType == 3 {
			break
		}

		if reply.TaskType == 0 {
			readfile, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("Error open %v: %v", reply.FileName, err)
			}
			content, err := io.ReadAll(readfile)
			if err != nil {
				log.Fatalf("Error read %v: %v", reply.FileName, err)
			}
			readfile.Close()

			kva := mapf(reply.FileName, string(content))

			tempfiles := make([]*os.File, reply.ReduceNum)
			encoders := make([]*json.Encoder, reply.ReduceNum)

			for i := 0; i < reply.ReduceNum; i++ {
				tempname := "mr-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(i) + "_"
				tempfiles[i], err = os.CreateTemp("", tempname)
				if err != nil {
					log.Fatalf("Error create %v: %v", tempname, err)
				}
				encoders[i] = json.NewEncoder(tempfiles[i])
			}

			for _, kv := range kva {
				err := encoders[ihash(kv.Key)%reply.ReduceNum].Encode(&kv)
				if err != nil {
					log.Fatalf("Error encoding: %v", err)
				}
			}

			for i, tempfile := range tempfiles {
				oname := "mr-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(i)
				os.Rename(tempfile.Name(), oname)
				tempfile.Close()
			}

			args.TaskId = reply.TaskId
			args.TaskType = reply.TaskType
			call("Coordinator.FinishTaskHandler", &args, &reply)
		} else if reply.TaskType == 1 {
			kva := []KeyValue{}
			for i := 0; i < reply.MapNum; i++ {
				tempname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskId)
				tempfile, err := os.Open(tempname)
				if err != nil {
					log.Fatalf("Error open %v: %v", tempfile, err)
				}

				decoder := json.NewDecoder(tempfile)
				for {
					var kv KeyValue
					err := decoder.Decode(&kv)
					if err == io.EOF {
						break
					}
					if err != nil {
						log.Fatalf("Error decoding: %v", err)
					}
					kva = append(kva, kv)
				}

				tempfile.Close()
			}

			sort.Sort(ByKey(kva))

			oname := "mr-out-" + strconv.Itoa(reply.TaskId)
			ofile, err := os.Create(oname)
			if err != nil {
				log.Fatalf("Error create %v: %v", oname, err)
			}

			for i := 0; i < len(kva); {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			ofile.Close()

			args.TaskId = reply.TaskId
			args.TaskType = reply.TaskType
			call("Coordinator.FinishTaskHandler", &args, &reply)
		} else if reply.TaskType == 2 {
			time.Sleep(time.Second)
		}
	}
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
