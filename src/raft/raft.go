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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
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

const (
	Follower = iota
	Candidate
	Leader
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

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state   int
	timer   time.Time
	applyCh chan ApplyMsg

	// persistent state on all servers
	currentTerm   int
	voteFor       int
	log           []LogEntry // valid index start from 1

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex   []int
	matchIndex  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil {
		panic("Having trouble read persist!")
	} else {
	  rf.currentTerm = currentTerm
	  rf.voteFor = voteFor
	  rf.log = log
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
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
	ConflictIndex int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		DPrintf(rf.me, "Handle RequestVote Request:\n\t%v\n\t%v", args.Format(), reply.Format())
		return
	}

	if rf.currentTerm < args.Term {
		rf.state = Follower
		rf.voteFor = -1
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}

	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		lastLogIndex := len(rf.log) - 1
		if rf.log[lastLogIndex].Term < args.LastLogTerm || (rf.log[lastLogIndex].Term == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) { // Election restriction
			rf.voteFor = args.CandidateId
			reply.VoteGranted = true
			rf.timer = time.Now()
		}
	}

	rf.persist()
	DPrintf(rf.me, "Handle RequestVote Request:\n\t%v\n\t%v", args.Format(), reply.Format())
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = rf.commitIndex + 1

	if args.Term < rf.currentTerm {
		DPrintf(rf.me, "Handle AppendEntries Request:\n\t%v\n\t%v", args.Format(), reply.Format())
		return
	}

	if rf.currentTerm < args.Term {
		rf.state = Follower
		rf.voteFor = -1
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}

	rf.timer = time.Now()
	lastLogIndex := len(rf.log) - 1

	if lastLogIndex < args.PrevLogIndex {
		reply.ConflictIndex = lastLogIndex + 1
		DPrintf(rf.me, "Handle AppendEntries Request:\n\t%v\n\t%v", args.Format(), reply.Format())
		return		
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		for logId := args.PrevLogIndex; logId >= 0; logId-- {
			if rf.log[logId].Term != rf.log[args.PrevLogIndex].Term {
				reply.ConflictIndex = logId + 1
				break
			}
		}
		DPrintf(rf.me, "Handle AppendEntries Request:\n\t%v\n\t%v", args.Format(), reply.Format())
		return		
	}

	reply.Success = true
	rf.log = append(rf.log[:args.PrevLogIndex + 1], args.Entries...)

	if rf.commitIndex < args.LeaderCommit {
		if args.LeaderCommit < lastLogIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastLogIndex
		}
	}

	rf.persist()
	DPrintf(rf.me, "Handle AppendEntries Request:\n\t%v\n\t%v", args.Format(), reply.Format())
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	rf.persist()

	return len(rf.log) - 1, rf.currentTerm, true
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
	voteCnt := 0

	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == Follower || rf.state == Candidate {
			if int64(time.Since(rf.timer) / time.Millisecond) > int64(300 + (rand.Int63() % 300)) {
				rf.state = Candidate
				rf.currentTerm += 1
				rf.voteFor = rf.me
				rf.timer = time.Now()
				voteCnt = 1	// vote for self
				currentTerm := rf.currentTerm
				lastLogIndex := len(rf.log) - 1
				lastLogTerm := rf.log[lastLogIndex].Term
				DPrintf(rf.me, "Start Send VoteRequest:\n\t%v", rf.Format())

				for i := range rf.peers {
					if i != rf.me {
						go func(i int, currentTerm int, lastLogIndex int, lastLogTerm int) {
							args := RequestVoteArgs{Term: currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
							reply := RequestVoteReply{}
							
							if ok := rf.sendRequestVote(i, &args, &reply); !ok {
								return
							}

							rf.mu.Lock()
							defer rf.mu.Unlock()

							if reply.Term > rf.currentTerm {
								rf.state = Follower
								rf.currentTerm = reply.Term
								rf.voteFor = -1
								return
							}

							if rf.state == Candidate && args.Term == rf.currentTerm {										
								if reply.VoteGranted {
									voteCnt += 1
									if voteCnt > len(rf.peers) / 2 {
										voteCnt = 0
										rf.state = Leader
										for i := range rf.peers {
											rf.nextIndex[i] = len(rf.log)
											rf.matchIndex[i] = 0
										}
									}
								}
							}
						}(i, currentTerm, lastLogIndex, lastLogTerm)
					}
				}
			}
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) heartBeater() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			currentTerm := rf.currentTerm
			commitIndex := rf.commitIndex
			DPrintf(rf.me, "Start Send Heartbeats:\n\t%v", rf.Format())

			for i := range rf.peers {
				if i != rf.me {
					prevLogIndex := rf.nextIndex[i] - 1
					prevLogTerm := rf.log[prevLogIndex].Term
					entries := rf.log[prevLogIndex + 1:]

					go func(i int, currentTerm int, prevLogIndex int, prevLogTerm int, entries []LogEntry, commitIndex int) {
						retry:
						args := AppendEntriesArgs{Term: currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: entries, LeaderCommit: commitIndex}
						reply := AppendEntriesReply{}
						
						if ok := rf.sendAppendEntries(i, &args, &reply); !ok {
							return
						}

						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.currentTerm = reply.Term
							rf.voteFor = -1
							rf.mu.Unlock()
							return
						}
						
						if rf.state == Leader && args.Term == rf.currentTerm {
							if !reply.Success {
								rf.nextIndex[i] = reply.ConflictIndex
								currentTerm = rf.currentTerm
								prevLogIndex = rf.nextIndex[i] - 1
								prevLogTerm = rf.log[prevLogIndex].Term
								entries = rf.log[prevLogIndex + 1:]
								commitIndex = rf.commitIndex
								rf.mu.Unlock()
								goto retry
							}

							rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
							rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						}
						rf.mu.Unlock()
					}(i, currentTerm, prevLogIndex, prevLogTerm, entries, commitIndex)
				}
			}

			for logId := len(rf.log) - 1; rf.log[logId].Term == rf.currentTerm && logId > rf.commitIndex; logId-- {
				cnt := 1
				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= logId {
						cnt += 1
					}
				}

				if cnt > len(rf.peers) / 2 {
					rf.commitIndex = logId
					break
				}
			}
		}
		rf.mu.Unlock()

		// send heartbeat every 100ms
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		applyMsgs := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			applyMsg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
			applyMsgs = append(applyMsgs, applyMsg)
		}
		rf.mu.Unlock()

		for _, applyMsg := range applyMsgs {
			rf.applyCh <- applyMsg
			DPrintf(rf.me, "Apply Log:\n\t%v\n\t%v", applyMsg.Format(), rf.Format())
		}

		// try to apply the log to state machine every 100ms
		time.Sleep(time.Duration(100) * time.Millisecond)
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

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = []LogEntry{{Command: nil, Term: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.timer = time.Now()
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start heartBeater goroutine to start heartbeats (only leader do)
	go rf.heartBeater()
	
	// periodically apply logs to state machine
	go rf.applier()

	return rf
}
