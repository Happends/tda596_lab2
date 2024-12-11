package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Worker_struct struct {
	mutex       sync.Mutex
	Id          string
	FilesMapped []string
	Reduced     []int
}

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

func worker_map(mapf func(string, string) []KeyValue, reply *Reply, id string) (string, error) {

	_, err := os.Stat("./intermediate/")
	if err != nil {
		// fmt.Println("creating intermediate folder")
		os.Mkdir("./intermediate/", os.ModePerm)
	}

	file, err := os.Open(reply.File)
	if err != nil {
		log.Fatalf("cannot open %v", reply.File)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.File)
	}
	file.Close()

	mapf_output := mapf(reply.File, string(content))

	for i := 0; i < reply.NReduce; i++ {
		path := fmt.Sprintf("./intermediate/mr-%d-%s-%d", reply.ReduceNumberOrFileIndex, id, i)
		_, err := os.Stat(path)
		if err != nil {
			os.Remove(path)
		}
	}

	for _, kv := range mapf_output { // create temp files first, then os.rename

		path := fmt.Sprintf("./intermediate/mr-%d-%s-%d", reply.ReduceNumberOrFileIndex, id, ihash(kv.Key)%reply.NReduce)
		file, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)

		if err != nil {
			fmt.Println("File OPEN FAULT")
			file, err = os.Create(path)
			if err != nil {
				return "", err
			}
		}

		enc := json.NewEncoder(file)
		enc.Encode(&kv)
		file.Close()
	}

	return reply.File, nil

}

func worker_reduce(reducef func(string, []string) string, reply *Reply, calling_id string) (int, error) {
	files, err := os.ReadDir("./intermediate/")
	kva := []KeyValue{}
	if err != nil {
		fmt.Println("error when reading the intermediate directory")
	}
	for _, fileinfo := range files {
		filename := fileinfo.Name()
		filenumbers := strings.Split(filename, "-")
		number, _ := strconv.Atoi(filenumbers[len(filenumbers)-1])
		//filenumbers[len(filenumbers)-2] == Id &&
		if filenumbers[len(filenumbers)-2] == calling_id && int(number) == reply.ReduceNumberOrFileIndex {
			// fmt.Println("filename ", filename)
			file, err := os.Open("./intermediate/" + filename)
			if err != nil {
				fmt.Println("file can't be opened: ", filename)
			}
			defer file.Close()
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
	for fileIndex, id := range reply.Ids {
		if id == calling_id {
			continue
		}
		worker_reply := Worker_rpc_reply{}
		// fmt.Println("calling_id: ", calling_id, " id: ", id)
		index := 0
		for index < 5 {
			if !call_worker(id, "Worker_struct.GetMap", &Worker_rpc_args{Reduce_number: reply.ReduceNumberOrFileIndex, FileIndex: fileIndex}, &worker_reply) {
				// send message to coordinator about cant reach worker (perhaps after 10 tries)
				// fmt.Println("cant contact worker at the moment")
				index++
			} else {
				// fmt.Println("kva length: ", len(kva))
				for _, kv := range worker_reply.Kva {
					kva = append(kva, kv)
				}
				break
			}
		}
		if index == 5 {
			invalid_worker_reply := Reply{}
			call("Coordinator.Invalid_worker", &Args{Id: id}, &invalid_worker_reply)
			if (!call("Coordinator.Invalid_worker", &Args{Id: id}, &invalid_worker_reply) || reply.Cmd != 0) {
				fmt.Println("something scuffed with coordinator")
			}
			return -1, errors.New("invalid worker")
		}
	}
	sort.Sort(ByKey(kva))

	_, err = os.Stat("./intermediate_reduce/")
	if err != nil {
		// fmt.Println("creating intermediate folder")
		os.Mkdir("./intermediate_reduce/", os.ModePerm)
	}
	filename := fmt.Sprintf("./intermediate_reduce/mr-out-%s-%d", calling_id, reply.ReduceNumberOrFileIndex)
	ofile, _ := os.Create(filename)
	defer ofile.Close()

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	enc := json.NewEncoder(ofile)
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		// fmt.Println(kva[i].Key)
		// fmt.Println(values)
		output := reducef(kva[i].Key, values)
		// fmt.Println(output)

		// this is the correct format for each line of Reduce output.

		kv := KeyValue{kva[i].Key, output}
		enc.Encode(&kv)

		i = j
	}

	return reply.ReduceNumberOrFileIndex, nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := MakeWorker()

	// uncomment to send the Example RPC to the coordinator.
	for {
		fmt.Println("Getting work!");
		reply, err := get_work(w.Id)
		fmt.Println("Got work: ", w.Id);

		if err != nil {
			fmt.Println("error when getting work: ", err)
			return
		}

		switch reply.Cmd {
		case 0: // work done acc // shouldn't be possible but ignore for now
			// fmt.Println("0: acc wrongly sent")
			continue
		case 3: // finsihed
			// fmt.Println("work done!")
			return
		case 4:
			// fmt.Println("maps not finished, waiting")
			time.Sleep(1 * time.Second)
			continue

		case 1:
			// fmt.Println("map task started")
			filename, err := worker_map(mapf, reply, w.Id)
			if err != nil {
				fmt.Println("couldn't map, err: ", err)
				return
			}

			reply, err = work_done(reply, w.Id)
			if err != nil {
				fmt.Println("no reply from coordinator, worker exit, err: ", err)
				return
			}

			w.mutex.Lock()
			w.FilesMapped = append(w.FilesMapped, filename)
			w.mutex.Unlock()

		case 2:
			// fmt.Println("reduce task started")
			reduce_number, err := worker_reduce(reducef, reply, w.Id)
			if err != nil {
				fmt.Println("couldn't map, err: ", err)
				return
			}

			reply, err = work_done(reply, w.Id)
			if err != nil {
				fmt.Println("no reply from coordinator, worker exit, err: ", err)
				return
			}

			w.mutex.Lock()
			w.Reduced = append(w.Reduced, reduce_number)
			w.mutex.Unlock()
		}

	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func get_work(id string) (*Reply, error) {

	// declare an argument structure.
	args := Args{FileIndexOrReduceIndex: 0, Id: id}

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Want_work", &args, &reply)
	if ok {
		// fmt.Printf("reply.Cmd %v\n", reply.Cmd)
		// fmt.Printf("reply.Files %v\n", reply.File)
		// fmt.Printf("reply.ReduceNumberOrFileIndex %v\n", reply.ReduceNumberOrFileIndex)
		// fmt.Printf("reply.NReduce %v\n", reply.NReduce)

		return &reply, nil
	} else {
		fmt.Printf("call failed!\n")
		return nil, errors.New("call to coordinator failed")
	}
}

func work_done(reply *Reply, id string) (*Reply, error) {

	args := Args{Id: id, FileIndexOrReduceIndex: reply.ReduceNumberOrFileIndex}

	if reply.Cmd == 1 {
		args.Reduce = false
	} else if reply.Cmd == 2 {
		args.Reduce = true
	} else {
		return nil, errors.New("work done for neither map nor reduce")
	}

	// declare a reply structure.
	rep_ret := Reply{}

	index := 0
	for !call("Coordinator.Work_done", &args, &rep_ret) && index < 10 {
		time.Sleep(8 * time.Second)
		index += 1
	}

	if index == 10 {
		return nil, errors.New("couldnt confirm work with coordinator")
	}

	if rep_ret.Cmd == 0 && rep_ret.ReduceNumberOrFileIndex == 2 {
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
	c, err := rpc.DialHTTP("tcp", sockname)
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

func call_worker(id string, rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := workerSock(id)
	c, err := rpc.DialHTTP("tcp", sockname)
	if err != nil {
		fmt.Println("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (w *Worker_struct) GetMap(args *Worker_rpc_args, reply *Worker_rpc_reply) error {

	files, err := os.ReadDir("./intermediate/")
	kva := []KeyValue{}
	if err != nil {
		fmt.Println("error when reading the intermediate directory")
	}
	for _, fileinfo := range files {
		filename := fileinfo.Name()
		filenumbers := strings.Split(filename, "-")
		reduce_number, _ := strconv.Atoi(filenumbers[len(filenumbers)-1])
		fileIndex, _ := strconv.Atoi(filenumbers[len(filenumbers)-3])

		if args.FileIndex == fileIndex && filenumbers[len(filenumbers)-2] == w.Id && reduce_number == args.Reduce_number {
			// fmt.Println("filename ", filename)
			file, err := os.Open("./intermediate/" + filename)
			if err != nil {
				fmt.Println("file can't be opened: ", filename)
				continue
			}
			defer file.Close()
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

	reply.Kva = kva
	// fmt.Println("kva: ", kva)

	return nil
}

func (w *Worker_struct) GetReduce(args *Worker_rpc_args, reply *Worker_rpc_reply) error {
	files, err := os.ReadDir("./intermediate_reduce/")
	kva := []KeyValue{}
	if err != nil {
		fmt.Println("error when reading the intermediate directory")
	}
	for _, fileinfo := range files {
		filename := fileinfo.Name()
		filenumbers := strings.Split(filename, "-")
		reduce_number, _ := strconv.Atoi(filenumbers[len(filenumbers)-1])
		// fmt.Println("id: ", filenumbers[len(filenumbers)-2], " w.Id: ", w.Id)
		// fmt.Println("reduce_number : ", reduce_number, " args.Reduce_number: ", args.Reduce_number)
		if filenumbers[len(filenumbers)-2] == w.Id && reduce_number == args.Reduce_number {
			// fmt.Println("filename ", filename)
			file, err := os.Open("./intermediate_reduce/" + filename)
			if err != nil {
				fmt.Println("file can't be opened: ", filename)
				continue
			}
			defer file.Close()
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			break
		}
	}

	reply.Kva = kva
	// fmt.Println("reply bytes: ", reply.Kva)
	return nil
}

func (w *Worker_struct) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	sockname := workerSock(w.Id)
	// fmt.Println("worker sockname: ", sockname)
	l, e := net.Listen("tcp", sockname)
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeWorker() *Worker_struct {
	w := Worker_struct{}

	// Your code here.

	// fmt.Println("id: ", w.Id)
	w.Id = "129.16.121.79:" + strconv.Itoa(os.Getpid()%(math.MaxInt16+1))
	w.FilesMapped = []string{}
	w.Reduced = []int{}

	w.server()
	return &w
}
