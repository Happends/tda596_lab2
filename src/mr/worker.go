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
	reply, err := get_work()

	if err != nil {
		fmt.Println("error when getting work: ", err)
	}

	switch reply.Resp {
	case 1:

		_, err := os.Stat("./intermediate/")
		if err != nil {
			fmt.Println("creating intermediate folder")
			os.Mkdir("./intermediate/", os.ModePerm)
		}
		map_map := []KeyValue{}
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
		map_map = append(map_map, mapf_output...)

		groupedMap := make(map[string][]string)
		for _, kv := range map_map {
			groupedMap[kv.Key] = append(groupedMap[kv.Key], kv.Value)
		}

		path := fmt.Sprintf("./intermediate/mr-%s-%d", reply.File, reply.ReduceNumber)
		file, err = os.Create(path)
		if err != nil {
			fmt.Println("file can't be opened: ", path)
		}

		enc := json.NewEncoder(file)
		for _, kv := range map_map {
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
			if int(number) == reply.ReduceNumber {
				file, err := os.Open(filename)
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

		filename := fmt.Sprintf("mr-out-%s", reply.ReduceNumber)
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
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func get_work() (*Reply, error) {

	// declare an argument structure.
	args := Args{0, "hello"}

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
		fmt.Printf("reply.NReduce %v\n", reply.NReduce)
		fmt.Printf("reply.NReduce %v\n", reply.ReduceNumber)

		return &reply, nil
	} else {
		fmt.Printf("call failed!\n")
		return nil, errors.New("call to coordinator failed")
	}
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
