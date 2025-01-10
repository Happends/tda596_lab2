package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Command int

const (
	SET Command = 0
	ADD Command = 1
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	// temp
	leaderId int

	votes           int
	nAppliedEntries int
	inTimer         bool
	timer           *time.Timer

	commitupdateMutex *sync.Mutex
	commitupdateCond  *sync.Cond

	applyCh chan ApplyMsg
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int = rf.currentTerm
	var isleader bool = rf.me == rf.leaderId
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() == false {
		// Your code here (2A, 2B).
		// fmt.Println("request vote handler:", rf.me)
		rf.mu.Lock()
		// fmt.Println("request vote handler got lock:", rf.me)
		// fmt.Println("aquire lock requestvote handler:", rf.me)
		// fmt.Println("lastlogindex:", args.LastLogIndex, "lastapplied:", rf.lastApplied, "votedfor:", rf.votedFor, args.Term, rf.currentTerm)
		if args.Term < rf.currentTerm {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			return
		}
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}
		reply.VoteGranted = (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.LastLogIndex >= rf.lastApplied
		fmt.Println("me:", rf.me, "votegrant:", reply.VoteGranted, "term:", rf.currentTerm, "args term:", args.Term, "lastApplied", rf.lastApplied, "args lastApplied", args.LastLogIndex)

		if reply.VoteGranted {
			// fmt.Println("reset timer")
			rf.resetTimer()
			// fmt.Println("timer was reset")
			rf.votedFor = args.CandidateId
		}
		reply.Term = rf.currentTerm
		// fmt.Println("release request vote handler lock:", rf.me)
		rf.mu.Unlock()

	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// fmt.Println("in appendentries:", rf.me)
	if !rf.killed() {
		// fmt.Println("append entries handler, me:", rf.me)
		rf.mu.Lock()
		// fmt.Println("me:", rf.me, "terms", args.Term, rf.currentTerm, "prevTerms", rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		// fmt.Println("me:", rf.me, "rf.log len:", len(rf.log), "prevLogIndex:", args.PrevLogIndex)
		if args.Term < rf.currentTerm || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			rf.mu.Unlock()
			return
		}
		rf.resetTimer()
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		// fmt.Println("inside lock:", rf.me)
		// fmt.Println("me:", rf.me, "accept leader:", args.LeaderId, "leaderId before:", rf.leaderId)
		rf.leaderId = args.LeaderId
		// fmt.Println("leaderId", rf.leaderId)
		rf.votedFor = -1
		// fmt.Println("reseting timer:", rf.me)
		reply.Success = true

		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		// fmt.Println("me:", rf.me, "prevLogIndex:", args.PrevLogIndex, "len(entries):", len(args.Entries))

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
			rf.mu.Unlock()
			rf.commitupdateCond.Signal()
		} else {
			rf.mu.Unlock()
		}

		// fmt.Println("done reseting timer:", rf.me)
	}

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// fmt.Println("command:", command)
	rf.mu.Lock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.leaderId == rf.me

	// Your code here (2B).

	if isLeader {
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		rf.nextIndex[rf.me] = index + 1
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.leaderId = -1
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.

		// OBS: check createOrSetTimer & timerTimeout!

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 300 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntriesMacro() {
	for rf.leaderId == rf.me {
		rf.mu.Lock()
		if rf.killed() {
			rf.leaderId = -1
			rf.resetTimer()
			rf.mu.Unlock()
			break
		}
		fmt.Println("heartbeat: ", rf.me, "leaderId:", rf.leaderId)

		total := len(rf.peers)
		logLastIndex := len(rf.log) - 1
		rf.mu.Unlock()
		// var wg sync.WaitGroup
		ch := make(chan bool, total)
		calls := 0

		rf.nAppliedEntries = 0
		for i := range rf.peers {
			if i == rf.me {
				rf.mu.Lock()
				rf.nAppliedEntries += 1
				calls += 1
				rf.mu.Unlock()
				continue
			}
			args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: rf.commitIndex}
			args.PrevLogIndex = rf.nextIndex[i] - 1           // check later
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term // check later
			args.Entries = rf.log[rf.nextIndex[i]:]           // check later

			reply := AppendEntriesReply{}

			// wg.Add(1)
			// fmt.Println("me:", rf.me, "send ae:", i)600
			go func(id int) {
				// defer wg.Done()
				// fmt.Println("me:", rf.me, "send ae:", id)
				ok := rf.sendAppendEntries(id, &args, &reply)
				// fmt.Println("me:", rf.me, "after send ae:", id)
				rf.mu.Lock()
				if ok {
					if reply.Success && reply.Term == rf.currentTerm {
						rf.nAppliedEntries += 1
						rf.nextIndex[id] = logLastIndex + 1
						rf.matchIndex[id] = logLastIndex

						rf.commitupdateCond.Signal()
						// fmt.Println("id:", id, "nextIndex:", rf.nextIndex[id], "nextIndexes:", rf.nextIndex)
					} else if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
					}
					// fmt.Println("entries:", nAppliedEntries)
				} else {
					// total -= 1
				}
				calls += 1
				rf.mu.Unlock()
				ch <- true
			}(i)
		}

		for {
			<-ch
			// fmt.Println("APPLIED ENTRY RECIEVED")
			rf.mu.Lock()
			// fmt.Println("applied entry done:", rf.votes, "calls:", calls)
			if total-calls+rf.nAppliedEntries <= total/2 {
				// fmt.Println("nAppliedEntries fail me:", rf.me, "sum:", total-calls+rf.votes, "total/2:", total/2, "args.Term", args.Term)
				rf.leaderId = -1
				rf.resetTimer()
				rf.mu.Unlock()
				break
			} else if rf.nAppliedEntries > total/2 {
				// fmt.Println("Applied entries leader:", rf.me, "term:", rf.currentTerm)
				// FIX HANDLE WHEN APPLIED ENTRIES SUCCEED
				rf.commitIndex = logLastIndex
				rf.commitupdateCond.Signal()
				rf.mu.Unlock()
				break
				// set nextIndex
			}
			rf.mu.Unlock()
		}

		// rf.mu.Lock()
		// if rf.nAppliedEntries <= total/2 {
		// 	rf.leaderId = -1
		// }
		// rf.mu.Unlock()
		// fmt.Println("heartbeat done: ", rf.me, "leaderId:", rf.leaderId)
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) timerTimeout() {
	rf.mu.Lock()

	if rf.killed() || rf.inTimer {
		rf.resetTimer()
		rf.mu.Unlock()
		return
	}
	rf.inTimer = true
	// if rf.votedFor != -1 {
	// 	fmt.Println("me:", rf.me, "voted for:", rf.votedFor)
	// 	rf.votedFor = -1
	// 	rf.stopTimer = nil
	// 	rf.resetTimer(rf.timerTimeout)
	// 	rf.mu.Unlock()
	// 	return
	// }
	fmt.Println("timeout: ", rf.me, "currentTerm:", rf.currentTerm)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.mu.Unlock()
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.lastApplied, LastLogTerm: rf.currentTerm} // term && lastlogterm the same?

	rf.votes = 0
	total := len(rf.peers)

	// var wg sync.WaitGroup
	ch := make(chan bool, total)
	calls := 0

	for i := range rf.peers {
		if i == rf.me {
			rf.mu.Lock()
			rf.votes += 1
			calls += 1
			rf.mu.Unlock()
			// fmt.Println("votes me:", rf.me, "votes:", rf.votes)
			continue
		}
		// fmt.Println("	me:", rf.me, "peer :", i)
		reply := RequestVoteReply{}

		// wg.Add(1)
		go func(id int) {
			// defer wg.Done()
			ok := rf.sendRequestVote(id, &args, &reply)
			// fmt.Println("me:", rf.me, "peer:", id, "ok:", ok, "currentTerm:", rf.currentTerm, "reply.Term", reply.Term, "reply.VoteGranted", reply.VoteGranted)
			rf.mu.Lock()
			if ok && rf.currentTerm == reply.Term && reply.VoteGranted {
				// fmt.Println("voted me:", rf.me, "peer:", id, "ok:", ok, "currentTerm:", rf.currentTerm, "reply.Term", reply.Term, "reply.VoteGranted", reply.VoteGranted)
				// fmt.Println("votes address:", &rf.votes)
				rf.votes += 1
			} else if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.resetTimer()
			}
			calls += 1
			rf.mu.Unlock()
			ch <- true
			// fmt.Println("me:", rf.me, "votes: ", rf.votes, "total/2:", total/2)
		}(i)

		// fmt.Println("vote i:", i, "ok:", ok, "voteGranted:", reply.VoteGranted)
		// fmt.Println("votes:", votes, "total/2:", total/2)
		// fmt.Println("me:", rf.me, "i:", i)
	}

	// index := 1
	for {
		// fmt.Println("waiting for:", index)
		<-ch
		// index += 1
		// fmt.Println("VOTE RECIEVED me:", rf.me, "votes:", rf.votes, "calls:", calls)
		rf.mu.Lock()
		if total-calls+rf.votes <= total/2 || rf.currentTerm != args.Term || rf.votedFor != rf.me {
			// fmt.Println("vote failed me:", rf.me, "sum:", total-calls+rf.votes, "total/2:", total/2, "rf.currentTerm:", rf.currentTerm, "args.Term", args.Term, "votedFor;", rf.votedFor, "me:", rf.me)
			rf.votedFor = -1
			rf.inTimer = false
			rf.resetTimer()
			rf.mu.Unlock()
			break
		} else if rf.votes > total/2 {
			// fmt.Println("voted leader:", rf.me, "term:", rf.currentTerm)
			rf.leaderId = rf.me

			rf.inTimer = false
			rf.mu.Unlock()
			go rf.sendAppendEntriesMacro()
			break
			// set nextIndex
		}
		rf.mu.Unlock()
		fmt.Println("timeout:", rf.me, "votes:", rf.votes, "total/2", total/2)
	}

	// wg.Wait()
	// fmt.Println("waited!", rf.me)

	// rf.mu.Lock()
	// if rf.votes > total/2 && rf.currentTerm == args.Term {
	// 	fmt.Println("voted leader:", rf.me, "term:", rf.currentTerm)
	// 	rf.leaderId = rf.me

	// 	rf.inTimer = false
	// 	rf.mu.Unlock()
	// 	go rf.sendAppendEntriesMacro()
	// 	// set nextIndex
	// } else {
	// 	rf.votedFor = -1
	// 	defer rf.resetTimer(rf.timerTimeout)
	// 	rf.inTimer = false
	// 	rf.mu.Unlock()
	// }
	// fmt.Println("timeout done: ", rf.me)
}

func (rf *Raft) resetTimer() {

	if rf.inTimer {
		return
	}

	ms := 250 + (rand.Int63() % 300)
	if !rf.timer.Reset(time.Duration(ms) * time.Millisecond) {
		rf.timer = time.AfterFunc(time.Duration(ms)*time.Millisecond, rf.timerTimeout)
	}

}

func (rf *Raft) checkCommitAndMatch() {
	for {
		// fmt.Println("waiting")
		rf.commitupdateMutex.Lock()
		rf.commitupdateCond.Wait()
		rf.commitupdateMutex.Unlock()
		// fmt.Println("checking commit")

		rf.mu.Lock()

		if rf.leaderId == rf.me && len(rf.matchIndex) > 0 {
			lowestIndex := -1
			for i, index := range rf.matchIndex {
				if i == 0 {
					lowestIndex = index
				} else {
					lowestIndex = min(lowestIndex, index)
				}
			}
			if lowestIndex > rf.commitIndex {
				for !(rf.currentTerm == rf.log[lowestIndex].Term || lowestIndex == rf.commitIndex) {
					lowestIndex--
				}
				rf.commitIndex = lowestIndex
			}
		}

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
			// fmt.Println("me:", rf.me, "applymsg:", rf.log[rf.lastApplied].Command)
		}

		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = []LogEntry{{Term: 0}}
	rf.matchIndex = []int{}
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.nextIndex = []int{}

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
	}

	rf.leaderId = -1
	rf.votes = 0
	rf.nAppliedEntries = 0
	rf.inTimer = false

	rf.applyCh = applyCh

	var mutex sync.Mutex
	rf.commitupdateMutex = &mutex
	rf.commitupdateCond = sync.NewCond(rf.commitupdateMutex)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.timer = time.AfterFunc(time.Duration(5)*time.Millisecond, rf.timerTimeout)
	rf.resetTimer()
	// go rf.ticker()

	go rf.checkCommitAndMatch()

	return rf
}
