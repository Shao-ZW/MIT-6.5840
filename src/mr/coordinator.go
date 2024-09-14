package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"


type Coordinator struct {
	// Your definitions here.
	files         []string
	mapNum        int
	reduceNum     int
	mapStatus     []int // 0 not assigned 1 doing 2 done
	reduceStatus  []int
	mapDoneCnt    int
	reduceDoneCnt int
	mutex         sync.Mutex
}

func (c *Coordinator) FinishTaskHandler(args *Args, reply *Reply) error {
	c.mutex.Lock()
	if args.TaskType == 0 {
		c.mapDoneCnt++
		c.mapStatus[args.TaskId] = 2
	} else if args.TaskType == 1 {
		c.reduceDoneCnt++
		c.reduceStatus[args.TaskId] = 2
	}
	c.mutex.Unlock()
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTaskHandler(args *Args, reply *Reply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.mapDoneCnt < c.mapNum {
		reply.TaskType = 2

		for id, status := range c.mapStatus {
			if status == 0 {
				reply.TaskType = 0
				reply.TaskId = id
				reply.MapNum = c.mapNum
				reply.ReduceNum = c.reduceNum
				reply.FileName = c.files[id]

				c.mapStatus[id] = 1

				go func() {
					time.Sleep(time.Duration(10) * time.Second)
					c.mutex.Lock()
					if c.mapStatus[id] != 2 {
						c.mapStatus[id] = 0 // assume died
					}
					c.mutex.Unlock()
				}()

				break
			}
		}
	} else if c.mapDoneCnt == c.mapNum && c.reduceDoneCnt < c.reduceNum {
		reply.TaskType = 2

		for id, status := range c.reduceStatus {
			if status == 0 {
				reply.TaskType = 1
				reply.TaskId = id
				reply.MapNum = c.mapNum
				reply.ReduceNum = c.reduceNum

				c.reduceStatus[id] = 1

				go func() {
					time.Sleep(time.Duration(10) * time.Second)
					c.mutex.Lock()
					if c.reduceStatus[id] != 2 {
						c.reduceStatus[id] = 0 // assume died
					}
					c.mutex.Unlock()
				}()

				break
			}
		}
	} else {
		reply.TaskType = 3
	}

	return nil
}

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
	// Your code here.
	c.mutex.Lock()
	ret := c.reduceDoneCnt == c.reduceNum
	c.mutex.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, mapNum: len(files), reduceNum: nReduce, mapDoneCnt: 0, reduceDoneCnt: 0}
	c.mapStatus = make([]int, c.mapNum)
	c.reduceStatus = make([]int, c.reduceNum)

	c.server()
	return &c
}
