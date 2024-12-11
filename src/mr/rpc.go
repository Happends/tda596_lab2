package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Args struct {
	Id                     string
	FileIndexOrReduceIndex int // if done fileindex says what fileindex is done
	Reduce                 bool
}

type Reply struct {
	Cmd                     int
	File                    string
	ReduceNumberOrFileIndex int
	NReduce                 int
	Ids                     map[int]string
}

// Add your RPC definitions here.

type Worker_rpc_args struct {
	Reduce_number int
	FileIndex     int
}

type Worker_rpc_reply struct {
	Kva []KeyValue
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "129.16.121.79:3000"
	return s
}

func workerSock(id string) string {
	return id
}
