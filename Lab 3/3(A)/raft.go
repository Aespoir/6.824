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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	FOLLOWER  int8 = 1
	LEADER    int8 = 2
	CANDIDATE int8 = 3

	BEGINTERM = 1

	VOTEDFORNIL = -1

	NOLEADER = -1

	ELECTIONTIMEOUT  int64 = 300
	HEARTBEATTIMEOUT int64 = 50
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	identity    int8
	currentTerm int
	votedFor    int

	// follower
	leaderID int

	// channels
	heartbeat         chan struct{}
	terminateElection chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.identity == LEADER

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
	// Your code here (3C).
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
	// Your code here (3C).
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
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []ApplyMsg
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm

	if len(args.Entries) == 0 { // just for heartbeat
		if rf.currentTerm > args.Term {
			reply.Success = false
			rf.mu.Unlock()
			return
		}
		if rf.identity == CANDIDATE {
			rf.identity = FOLLOWER
			rf.terminateElection <- struct{}{}
		} else if rf.identity == LEADER {
			rf.identity = FOLLOWER
		}
		rf.leaderID = args.LeaderID
		rf.currentTerm = args.Term
		reply.Success = true
		rf.mu.Unlock()
		rf.heartbeat <- struct{}{}
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateID
	rf.leaderID = args.CandidateID
	rf.currentTerm = args.Term
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		_, isLeader := rf.GetState()
		if isLeader {
			ms := HEARTBEATTIMEOUT*2 + (rand.Int63() % HEARTBEATTIMEOUT) // at most 10 times heartbeat per second
			heartbeatTimeout := time.After(time.Duration(ms) * time.Millisecond)
			select {
			case <-heartbeatTimeout:
				rf.startHeartbeat()
			}
		} else {
			ms := ELECTIONTIMEOUT*4 + (rand.Int63() % ELECTIONTIMEOUT) // 1200-1600
			electionTimeout := time.After(time.Duration(ms) * time.Millisecond)
			select {
			case <-electionTimeout:
				rf.startElection(ms)
			case <-rf.heartbeat:
			}
		}
	}
}

func (rf *Raft) startHeartbeat() {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &AppendEntriesArgs{Term: term, LeaderID: rf.me}
			reply := &AppendEntriesReply{}

			rf.sendAppendEntries(server, args, reply)
		}(i)
	}

}

func (rf *Raft) startElection(ms int64) {
	rf.mu.Lock()
	rf.identity = CANDIDATE
	rf.currentTerm++
	term := rf.currentTerm
	rf.votedFor = rf.me
	rf.mu.Unlock()

	currentVotes := 1
	require := len(rf.peers)>>1 + 1
	vote := make(chan struct{}, require)
	stop := make(chan struct{}, require)
	electionTimeout := time.After(time.Duration(ms*3) * time.Millisecond)

	go func() {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(server int) {
				args := &RequestVoteArgs{Term: term, CandidateID: rf.me}
				reply := &RequestVoteReply{}

				rf.sendRequestVote(server, args, reply)

				if reply.VoteGranted {
					vote <- struct{}{}
				}

				if reply.Term > term {
					rf.mu.Lock()
					rf.identity = FOLLOWER
					rf.currentTerm = reply.Term
					rf.leaderID = NOLEADER
					rf.votedFor = VOTEDFORNIL
					stop <- struct{}{}
				}
			}(i)
		}
	}()

	for currentVotes < require {
		select {
		case <-vote:
			currentVotes++
		case <-electionTimeout:
			rf.mu.Lock()
			rf.identity = FOLLOWER
			rf.votedFor = VOTEDFORNIL
			rf.leaderID = NOLEADER
			rf.mu.Unlock()
			return
		case <-stop:
			return
		case <-rf.terminateElection:
			return
		}
	}

	// win this election

	rf.mu.Lock()
	rf.identity = LEADER
	rf.leaderID = rf.me
	go rf.startHeartbeat()
	rf.mu.Unlock()
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

	// Your initialization code here (3A, 3B, 3C).
	rf.identity = FOLLOWER
	rf.currentTerm = BEGINTERM
	rf.votedFor = VOTEDFORNIL
	rf.leaderID = NOLEADER

	rf.heartbeat = make(chan struct{})
	rf.terminateElection = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
