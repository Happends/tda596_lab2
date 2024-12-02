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
	mu           sync.Mutex
	Files        []string
	FilesMapSent []bool
	FilesMapped  []bool
	FileIndex    int
	NReduce      int
	ReduceSent   []bool
	ReduceDone   []bool
	ReduceIndex  int
}

type Reply struct {
	Cmd                     int
	File                    string
	ReduceNumberOrFileIndex int
	NReduce                 int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Respond(args *Args, reply *Reply) error {
	// fmt.Println("got message")
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Cmd == 2 {
		if args.Reduce == false {
			c.FilesMapped[args.FileIndexOrReduceIndex] = true
		} else {
			c.ReduceDone[args.FileIndexOrReduceIndex] = true
		}
		reply.Cmd = 0
		reply.ReduceNumberOrFileIndex = args.Cmd
		return nil
	} else if args.Cmd == 1 {

		index, file, err := c.get_file()

		if err == nil {
			// fmt.Println("index: ", index)
			reply.Cmd = 1
			reply.File = file
			reply.ReduceNumberOrFileIndex = index
			reply.NReduce = c.NReduce
			c.FilesMapSent[index] = true
			go c.time_out(false, index)
			return nil
		}

		for i := 0; i < len(c.FilesMapped); i++ {
			if !c.FilesMapped[i] {
				reply.Cmd = 4
				reply.File = ""
				for j := 0; j < len(c.FilesMapSent); j++ {
					if !c.FilesMapSent[j] {
						c.FilesMapSent[j] = true
						reply.Cmd = 1
						reply.File = c.Files[j]
						reply.ReduceNumberOrFileIndex = j
						reply.NReduce = c.NReduce
						go c.time_out(false, j)
						break
					}
				}
				return nil
			}
		}

		reduceN, err := c.get_reduce_number()

		if err == nil {
			fmt.Println("reduceN: ", reduceN)
			reply.Cmd = 2
			reply.ReduceNumberOrFileIndex = reduceN
			reply.NReduce = c.NReduce
			c.ReduceSent[reduceN] = true
			go c.time_out(true, reduceN)
			if reduceN == c.NReduce {
				return errors.New("Reduce number equal to coordinator number")
			}
			return nil
		}

		for i := 0; i < len(c.ReduceDone); i++ {
			if !c.ReduceDone[i] {
				reply.Cmd = 4
				reply.File = ""
				for j := 0; j < len(c.ReduceSent); j++ {
					if !c.ReduceSent[j] {
						c.ReduceSent[j] = true
						reply.Cmd = 2
						reply.File = ""
						reply.ReduceNumberOrFileIndex = j
						reply.NReduce = c.NReduce
						go c.time_out(true, j)
						break
					}
				}
				return nil
			}
		}

		reply.Cmd = 3
		reply.File = ""

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
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false

	if c.FileIndex == len(c.Files) && c.ReduceIndex == c.NReduce {
		fmt.Println("Coordinator done")
		ret = true
	}
	// Your code here.

	if ret == true {
		for _, b := range c.FilesMapped {
			if b == false {
				return false
			}
		}

		for _, b := range c.ReduceDone {
			if b == false {
				return false
			}
		}

	}

	return ret
}

func (c *Coordinator) get_file() (int, string, error) {
	if c.FileIndex >= len(c.Files) {
		return 0, "", errors.New("No more files to map")
	}

	ret := c.Files[c.FileIndex]
	c.FileIndex += 1
	return c.FileIndex - 1, ret, nil
}

func (c *Coordinator) get_file_index(index int) (string, error) {
	if index >= len(c.Files) {
		return "", errors.New("map index outside of files")
	}

	ret := c.Files[index]
	return ret, nil
}

func (c *Coordinator) get_reduce_number() (int, error) {
	if c.ReduceIndex >= c.NReduce {
		return -1, errors.New("No more reduce tasks")
	}

	c.ReduceIndex += 1
	return c.ReduceIndex - 1, nil
}

func (c *Coordinator) reduce_number_allowed(reduceN int) (int, error) {
	if reduceN >= c.NReduce {
		return -1, errors.New("max reduce: " + string(reduceN))
	}

	return reduceN, nil
}

func (c *Coordinator) time_out(reduce bool, index int) {
	time.Sleep(time.Second * 10)
	c.mu.Lock()
	if reduce && !c.ReduceDone[index] {
		c.ReduceSent[index] = false
	} else if !reduce && !c.FilesMapped[index] {
		c.FilesMapSent[index] = false
	}
	c.mu.Unlock()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	fmt.Println("create coordinator: \nFiles: ", files, "\nNReduce: ", nReduce)

	c.Files = files
	c.FilesMapSent = make([]bool, len(files))
	for i := 0; i < len(c.FilesMapped); i++ {
		c.FilesMapSent[i] = false
	}
	c.FilesMapped = make([]bool, len(files))
	for i := 0; i < len(c.FilesMapped); i++ {
		c.FilesMapped[i] = false
	}
	c.FileIndex = 0
	fmt.Println("nReduce: ", nReduce)
	c.NReduce = nReduce
	c.ReduceSent = make([]bool, nReduce)
	for i := 0; i < len(c.ReduceSent); i++ {
		c.ReduceSent[i] = false
	}
	c.ReduceDone = make([]bool, nReduce)
	for i := 0; i < len(c.ReduceDone); i++ {
		c.ReduceDone[i] = false
	}
	c.ReduceIndex = 0

	// Your code here.

	c.server()
	return &c
}
