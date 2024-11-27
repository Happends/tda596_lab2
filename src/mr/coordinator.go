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
)

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	Files       []string
	FilesMapped []bool
	FileIndex   int
	NReduce     int
	ReduceIndex int
}

type Reply struct {
	Resp                      int
	File                      string
	WorkerIndexOrReduceNumber int
	NReduce                   int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Respond(args *Args, reply *Reply) error {
	if args.Arg == "Done with work" {
		c.FilesMapped[args.FileIndex] = true
		reply.Resp = 0
		reply.NReduce = args.Cmd
		return nil
	} else if args.Arg == "I want work" {

		c.mu.Lock()
		file, err := get_file(c)
		defer c.mu.Unlock()

		if err == nil {
			reply.Resp = 1
			reply.File = file
			reply.WorkerIndexOrReduceNumber = c.FileIndex - 1
			reply.NReduce = c.NReduce
			return nil
		}

		for i := 0; i < len(c.FilesMapped); i++ {
			if !c.FilesMapped[i] {
				reply.Resp = 0
				reply.File = "Maps not finished"
				return nil
			}
		}

		reduceN, err := get_reduce_number(c)

		if err == nil {
			reply.Resp = 2
			reply.WorkerIndexOrReduceNumber = reduceN
			reply.NReduce = c.NReduce
			return nil
		}

		reply.Resp = 0
		reply.File = "No more work"

		return nil
	}
	return errors.New("Invalid arg string")
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	if c.FileIndex == len(c.Files) && c.ReduceIndex == c.NReduce {
		fmt.Println("Coordinator done")
		ret = true
	}
	// Your code here.

	return ret
}

func get_file(c *Coordinator) (string, error) {
	if c.FileIndex >= len(c.Files) {
		return "", errors.New("No more files to map")
	}

	ret := c.Files[c.FileIndex]
	c.FileIndex += 1
	return ret, nil
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

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	fmt.Println("create coordinator: \nFiles: ", files, "\nNReduce: ", nReduce)

	c.Files = files
	c.FilesMapped = make([]bool, len(files))
	for i := 0; i < len(c.FilesMapped); i++ {
		c.FilesMapped[i] = false
	}
	c.FileIndex = 0
	c.NReduce = nReduce
	c.ReduceIndex = 0

	// Your code here.

	c.server()
	return &c
}
