package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mutex        sync.Mutex
	Files        []string
	FilesMapSent []bool
	FilesMapped  map[int]string
	FileIndex    int
	NReduce      int
	ReduceSent   []bool
	ReduceDone   map[int]string
	ReduceSaved  []bool
	ReduceIndex  int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Want_work(args *Args, reply *Reply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()


	fmt.Println("requested work by: ", args.Id);
	index, file, err := get_file(c)

	reply.Cmd = -1;
	reply.ReduceNumberOrFileIndex = -1;

	if err == nil {
		// fmt.Println("index: ", index)
		reply.Cmd = 1
		reply.File = file
		reply.ReduceNumberOrFileIndex = index
		reply.NReduce = c.NReduce
		c.FilesMapSent[index] = true
		go time_out(c, false, index)
		return nil
	}

	for i, id := range c.FilesMapped {
		if id == "" {
			reply.Cmd = 4
			reply.File = ""
				if !c.FilesMapSent[i] {
					reply.Cmd = 1
					reply.File = c.Files[i]
					reply.ReduceNumberOrFileIndex = i
					reply.NReduce = c.NReduce
					c.FilesMapSent[i] = true
					go time_out(c, false, i)
				}
			return nil
		}
	}

	reduceN, err := get_reduce_number(c)

	// fmt.Println("FilesMapped: ", c.FilesMapped)

	if err == nil {
		// fmt.Println("reduceN: ", reduceN)
		reply.Cmd = 2
		reply.File = ""
		reply.ReduceNumberOrFileIndex = reduceN
		reply.NReduce = c.NReduce
		reply.Ids = c.FilesMapped
		c.ReduceSent[reduceN] = true
		go time_out(c, true, reduceN)
		if reduceN == c.NReduce {
			return errors.New("Reduce number equal to coordinator number")
		}
		return nil
	}

	for i, id := range c.ReduceDone {
		if id == "" {
			reply.Cmd = 4
			reply.File = ""
				if !c.ReduceSent[i] {
					c.ReduceSent[i] = true
					reply.Cmd = 2
					reply.ReduceNumberOrFileIndex = i
					reply.NReduce = c.NReduce
					reply.Ids = c.FilesMapped
					go time_out(c, true, i)
				}
			return nil
		}
	}

	for reduce_number, id := range c.ReduceDone {
		if !c.ReduceSaved[reduce_number] && id == args.Id {
			get_reduce(reduce_number, args.Id)
			c.ReduceSaved[reduce_number] = true
		}
	}
	reply.Cmd = 3
	reply.File = ""
	// fmt.Println("sending term id: ", args.Id)
	for _, saved := range c.ReduceSaved {
		if !saved {
			reply.Cmd = 4
		}
	}

	return nil
}

func (c *Coordinator) Work_done(args *Args, reply *Reply) error {

	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.Reduce == false {
		fmt.Println("mapped: ", args.FileIndexOrReduceIndex, "by: ", args.Id);
		c.FilesMapped[args.FileIndexOrReduceIndex] = args.Id
	} else {
		fmt.Println("reduced: ", args.FileIndexOrReduceIndex, "by: ", args.Id);
		c.ReduceDone[args.FileIndexOrReduceIndex] = args.Id
	}
	reply.Cmd = 0
	reply.ReduceNumberOrFileIndex = 2

	return nil

}

func (c *Coordinator) Invalid_worker(args *Args, reply *Reply) error {
	fmt.Println("worker invalidated: ", args.Id);
	for i, id := range c.FilesMapped {
		fmt.Println("worker id: ", id);
		if id == args.Id {
			fmt.Println("worker id found:");
			c.FilesMapped[i] = ""
			c.FilesMapSent[i] = false
		}
	}
	for i, id := range c.ReduceDone {
		if id == args.Id {
			c.ReduceDone[i] = ""
			c.ReduceSent[i] = false
		}
	}

	fmt.Println("worker Invalidated: ", args.Id)
	reply.Cmd = 0
	return nil
}

func get_reduce(reduce_number int, id string) {

	index := 0
	for index < 10 {
		worker_reply := Worker_rpc_reply{}
		if !call_worker(id, "Worker_struct.GetReduce", &Worker_rpc_args{Reduce_number: reduce_number}, &worker_reply) {
			index++
			fmt.Println("cant call")
		} else {
			filename := fmt.Sprintf("./mr-out-%d", reduce_number)
			ofile, _ := os.Create(filename)
			for _, kv := range worker_reply.Kva {
				fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
			}
			index = 0
			break
		}
	}

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	// os.Remove(sockname)
	// fmt.Println("coordinator sockname: ", sockname)
	l, e := net.Listen("tcp", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, saved := range c.ReduceSaved {
		if !saved {
			return false
		}
	}

	return true
}

func get_file(c *Coordinator) (int, string, error) {
	if c.FileIndex >= len(c.Files) {
		return 0, "", errors.New("No more files to map")
	}

	ret := c.Files[c.FileIndex]
	c.FileIndex += 1
	return c.FileIndex - 1, ret, nil
}

func get_file_index(c *Coordinator, index int) (string, error) {
	if index >= len(c.Files) {
		return "", errors.New("map index outside of files")
	}

	ret := c.Files[index]
	return ret, nil
}

func get_reduce_number(c *Coordinator) (int, error) {
	if c.ReduceIndex >= c.NReduce {
		return -1, errors.New("No more reduce tasks")
	}

	c.ReduceIndex += 1
	return c.ReduceIndex - 1, nil
}

func reduce_number_allowed(c *Coordinator, reduceN int) (int, error) {
	if reduceN >= c.NReduce {
		return -1, errors.New("max reduce: " + string(reduceN))
	}

	return reduceN, nil
}

func time_out(c *Coordinator, reduce bool, index int) {
	time.Sleep(time.Second * 10)
	c.mutex.Lock()
	if reduce && c.ReduceDone[index] == "" {
		c.ReduceSent[index] = false
	} else if !reduce && c.FilesMapped[index] == "" {
		c.FilesMapSent[index] = false
	}
	c.mutex.Unlock()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// fmt.Println("create coordinator: \nFiles: ", files, "\nNReduce: ", nReduce)

	c.Files = files
	c.FilesMapSent = make([]bool, len(files))
	for i := 0; i < len(c.FilesMapped); i++ {
		c.FilesMapSent[i] = false
	}
	c.FilesMapped = map[int]string{}
	for i := 0; i < len(files); i++ {
		c.FilesMapped[i] = ""
	}
	c.FileIndex = 0
	// fmt.Println("nReduce: ", nReduce)
	c.NReduce = nReduce
	c.ReduceSent = make([]bool, nReduce)
	for i := 0; i < len(c.ReduceSent); i++ {
		c.ReduceSent[i] = false
	}
	c.ReduceDone = map[int]string{}
	for i := 0; i < nReduce; i++ {
		c.ReduceDone[i] = ""
	}
	c.ReduceSaved = make([]bool, nReduce)
	for i := 0; i < len(c.ReduceSaved); i++ {
		c.ReduceSaved[i] = false
	}
	c.ReduceIndex = 0

	// Your code here.

	c.server()
	return &c
}
