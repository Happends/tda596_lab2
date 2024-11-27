package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
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
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		reply, err := get_work()

		if err != nil {
			fmt.Println("error when getting work: ", err)
			return
		}

		switch reply.Resp {
		case 0:
			fmt.Println("0")
			if reply.File == "Maps not finished" {
				fmt.Println("maps not finished")
				time.Sleep(2 * time.Second)
				continue
			} else if reply.File == "No more work" {
				fmt.Println("work done! exiting!")
				return
			}
		case 1:

			_, err := os.Stat("./intermediate/")
			if err != nil {
				fmt.Println("creating intermediate folder")
				os.Mkdir("./intermediate/", os.ModePerm)
			}

			file, err := os.Open(reply.File)
			if err != nil {
				log.Fatalf("cannot open %v", reply.File)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.File)
			}
			file.Close()

			mapf_output := mapf(reply.File, string(content))

			for _, kv := range mapf_output {

				path := fmt.Sprintf("./intermediate/mr-%d-%d", reply.WorkerIndexOrReduceNumber, ihash(kv.Key)%reply.NReduce)
				file, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)

				if err != nil {
					fmt.Println("File OPEN FAULT")
					file, err = os.Create(path)
					if err != nil {
						fmt.Println("WEIRD FILE CRASH, CANT OPEN, CANT CREATE: ", path)
						return
					}
				}

				enc := json.NewEncoder(file)
				enc.Encode(&kv)
			}
			file.Close()

		case 2:

			files, err := ioutil.ReadDir("./intermediate/")
			kva := []KeyValue{}
			if err != nil {
				fmt.Println("error when reading the intermediate directory")
			}
			for _, fileinfo := range files {
				filename := fileinfo.Name()
				filenumbers := strings.Split(filename, "-")
				number, _ := strconv.Atoi(filenumbers[len(filenumbers)-1])

				if int(number) == reply.WorkerIndexOrReduceNumber {
					fmt.Println("filename ", filename)
					file, err := os.Open("./intermediate/" + filename)
					if err != nil {
						fmt.Println("file can't be opened: ", file)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				}
			}
			sort.Sort(ByKey(kva))

			filename := fmt.Sprintf("mr-out-%d", reply.WorkerIndexOrReduceNumber)
			ofile, _ := os.Create(filename)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			ofile.Close()
		}

		reply, err = work_done(reply)
		if err != nil {
			fmt.Println("worker crash: ", err)
			return
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func get_work() (*Reply, error) {

	// declare an argument structure.
	args := Args{}

	// fill in the argument(s).
	args.Cmd = 1
	args.Arg = "I want work"

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Respond", &args, &reply)
	if ok {
		fmt.Printf("reply.resp %v\n", reply.Resp)
		fmt.Printf("reply.Files %v\n", reply.File)

		return &reply, nil
	} else {
		fmt.Printf("call failed!\n")
		return nil, errors.New("call to coordinator failed")
	}
}

func work_done(reply *Reply) (*Reply, error) {

	// declare an argument structure.
	args := Args{Cmd: reply.Resp, Arg: "Done with work", FileIndex: reply.WorkerIndexOrReduceNumber}

	// declare a reply structure.
	rep_ret := Reply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	index := 0
	for !call("Coordinator.Respond", &args, &rep_ret) && index < 10 {
		time.Sleep(8 * time.Second)
		index += 1
	}

	if index == 10 {
		return nil, errors.New("couldnt confirm work with coordinator")
	}

	if rep_ret.Resp == 0 && rep_ret.NReduce == args.Cmd {
		return &rep_ret, nil
	}
	return nil, errors.New("wrong response to worker work confirmation")
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
