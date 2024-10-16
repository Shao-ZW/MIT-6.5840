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
	"log"
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
	Index   int
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

	state         int
	timer         time.Time
	applyCh       chan ApplyMsg
	heartbeatCond *sync.Cond
	applyCond     *sync.Cond

	// persistent state on all servers
	currentTerm   int
	voteFor       int
	log           []LogEntry // should always leave at least one entry
	snapshotIndex int
	snapshotTerm  int
	snapshot      []byte

	// volatile state on all servers
	commitIndex int
	lastApplied int
	voteCnt     int

	// volatile state on leaders
	nextIndex   []int
	matchIndex  []int

	// Debug log
	logger *log.Logger
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
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
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
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil || d.Decode(&snapshotIndex) != nil || d.Decode(&snapshotTerm) != nil {
		panic("Having trouble read persist!")
	} else {
	  rf.currentTerm = currentTerm
	  rf.voteFor = voteFor
	  rf.log = log
	  rf.snapshotIndex = snapshotIndex
	  rf.snapshotTerm = snapshotTerm
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.snapshotIndex { 
		return
	}

	logPos := index - rf.log[0].Index
	
	rf.snapshot = snapshot
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.log[logPos].Term
	rf.log = rf.log[logPos:]
	rf.persist()
	DPrintf(rf.logger, "Receive Snapshot:\n\t%v %v\n\t%v", index, snapshot, rf.Format())
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
	Term          int
	Success       bool
	ConflictIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		DPrintf(rf.logger, "Handle RequestVote Request from server %v:\n\t%v\n\t%v\n\t%v", args.CandidateId, args.Format(), reply.Format(), rf.Format())
		return
	}

	if rf.currentTerm < args.Term {
		rf.state = Follower
		rf.voteFor = -1
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}

	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		lastLogIndex := rf.log[len(rf.log) - 1].Index
		lastLogTerm := rf.log[len(rf.log) - 1].Term
		if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) { // Election restriction
			rf.voteFor = args.CandidateId
			reply.VoteGranted = true
			rf.timer = time.Now()
		}
	}

	rf.persist()
	DPrintf(rf.logger, "Handle RequestVote Request from server %v:\n\t%v\n\t%v\n\t%v", args.CandidateId, args.Format(), reply.Format(), rf.Format())
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = rf.commitIndex + 1

	if args.Term < rf.currentTerm {
		DPrintf(rf.logger, "Handle AppendEntries Request from server %v:\n\t%v\n\t%v\n\t%v", args.LeaderId, args.Format(), reply.Format(), rf.Format())
		return
	}

	if rf.currentTerm < args.Term {
		rf.state = Follower
		rf.voteFor = -1
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}

	lastLogIndex := rf.log[len(rf.log) - 1].Index
	rf.timer = time.Now()

	// consistency check
	if lastLogIndex < args.PrevLogIndex {
		reply.ConflictIndex = lastLogIndex + 1
		DPrintf(rf.logger, "Handle AppendEntries Request from server %v:\n\t%v\n\t%v\n\t%v", args.LeaderId, args.Format(), reply.Format(), rf.Format())
		return		
	}

	logPos := args.PrevLogIndex - rf.log[0].Index

	if logPos >= 0 {
		logTerm := rf.log[logPos].Term
		if logTerm != args.PrevLogTerm {
			for i := logPos; i >= 0; i-- {
				if rf.log[i].Term != logTerm {
					reply.ConflictIndex = rf.log[i].Index + 1
					break
				}
			}
			DPrintf(rf.logger, "Handle AppendEntries Request from server %v:\n\t%v\n\t%v\n\t%v", args.LeaderId, args.Format(), reply.Format(), rf.Format())
			return		
		}
	}

	reply.Success = true
	
	if logPos < 0 {
		return
	}

	// handle duplicate RPC requests
	// rf.log = append(rf.log[:logPos + 1], args.Entries...) // only this line is wrong!
	for i, entry := range args.Entries {
		logPos++
		if logPos >= len(rf.log) || rf.log[logPos].Term != entry.Term {
			rf.log = append(rf.log[:logPos], args.Entries[i:]...)
			break
		}
	}

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, lastLogIndex)
		rf.applyCond.Signal()
	}

	rf.persist()
	DPrintf(rf.logger, "Handle AppendEntries Request from server %v:\n\t%v\n\t%v\n\t%v", args.LeaderId, args.Format(), reply.Format(), rf.Format())
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		DPrintf(rf.logger, "Handle InstallSnapshot Request from server %v:\n\t%v\n\t%v\n\t%v", args.LeaderId, args.Format(), reply.Format(), rf.Format())
		return
	}

	if rf.currentTerm < args.Term {
		rf.state = Follower
		rf.voteFor = -1
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}

	rf.timer = time.Now()

	if args.LastIncludedIndex <= rf.snapshotIndex {
		DPrintf(rf.logger, "Handle InstallSnapshot Request from server %v:\n\t%v\n\t%v\n\t%v", args.LeaderId, args.Format(), reply.Format(), rf.Format())
		return
	}

	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshot = args.Data

	logPos := args.LastIncludedIndex - rf.log[0].Index

	if logPos < 0 || logPos >= len(rf.log) || rf.log[logPos].Term != args.LastIncludedTerm {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	} else {
		rf.log = rf.log[logPos:]
	}

	rf.applyCond.Signal()
	rf.persist()
	DPrintf(rf.logger, "Handle InstallSnapshot Request from server %v:\n\t%v\n\t%v\n\t%v", args.LeaderId, args.Format(), reply.Format(), rf.Format())
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)

	if !ok {
		DPrintf(rf.logger, "Can't Receive RequestVote Reply from server %v", server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.voteFor = -1
		DPrintf(rf.logger, "Handle RequestVote Reply from server %v:\n\t%v\n\t%v\n\t%v", server, args.Format(), reply.Format(), rf.Format())
		return
	}

	if rf.state == Candidate && args.Term == rf.currentTerm {										
		if reply.VoteGranted {
			rf.voteCnt += 1
			if rf.voteCnt > len(rf.peers) / 2 {
				rf.voteCnt = 0
				rf.state = Leader
				for i := range rf.peers {
					rf.nextIndex[i] = rf.log[len(rf.log) - 1].Index + 1
					rf.matchIndex[i] = 0
				}
			}
		}
	}

	DPrintf(rf.logger, "Handle RequestVote Reply from server %v:\n\t%v\n\t%v\n\t%v", server, args.Format(), reply.Format(), rf.Format())
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	
	if !ok {
		DPrintf(rf.logger, "Can't Receive AppendEntries Reply from server %v", server)
		return
	}
		
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.voteFor = -1
		DPrintf(rf.logger, "Handle AppendEntries Reply from server %v:\n\t%v\n\t%v\n\t%v", server, args.Format(), reply.Format(), rf.Format())
		return
	}
		
	if rf.state == Leader && args.Term == rf.currentTerm {
		if reply.Success {
			rf.nextIndex[server] = max(rf.nextIndex[server], args.PrevLogIndex + len(args.Entries) + 1)
			rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex + len(args.Entries))

			if logPos := rf.matchIndex[server] - rf.log[0].Index; logPos >= 0 && rf.log[logPos].Term == rf.currentTerm && rf.log[logPos].Index > rf.commitIndex {
				cnt := 1
				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= rf.matchIndex[server] {
						cnt += 1
					}
				}

				if cnt > len(rf.peers) / 2 {
					rf.commitIndex = rf.matchIndex[server]
					rf.applyCond.Signal()
				}
			}
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
			rf.heartbeatCond.Signal()
		}
	}
	
	DPrintf(rf.logger, "Handle AppendEntries Reply from server %v:\n\t%v\n\t%v\n\t%v", server, args.Format(), reply.Format(), rf.Format())
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

	if !ok {
		DPrintf(rf.logger, "Can't Receive InstallSnapshot Reply from server %v", server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader && args.Term == rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.voteFor = -1
		}
		rf.nextIndex[server] = max(rf.nextIndex[server], rf.snapshotIndex + 1)
		rf.matchIndex[server] = max(rf.matchIndex[server], rf.snapshotIndex)
	}

	DPrintf(rf.logger, "Handle InstallSnapshot Reply from server %v:\n\t%v\n\t%v\n\t%v", server, args.Format(), reply.Format(), rf.Format())
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

	logEntry := LogEntry{Command: command, Index: rf.log[len(rf.log) - 1].Index + 1, Term: rf.currentTerm}
	rf.log = append(rf.log, logEntry)
	rf.heartbeatCond.Signal()
	rf.persist()
	DPrintf(rf.logger, "Start: %v\n\t%v", command, rf.Format())
	return logEntry.Index, rf.currentTerm, true
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
				rf.voteCnt = 1	// vote for self
				lastLogIndex := rf.log[len(rf.log) - 1].Index
				lastLogTerm := rf.log[len(rf.log) - 1].Term
				
				for i := range rf.peers {
					if i != rf.me {
						args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
						DPrintf(rf.logger, "Start Send VoteRequest to server %v:\n\t%v\n\t%v", i, args.Format(), rf.Format())
						go rf.sendRequestVote(i, args)
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
	go func ()  {
		for !rf.killed() {
			rf.heartbeatCond.Signal()
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}()

	for !rf.killed() {
		rf.mu.Lock()
		rf.heartbeatCond.Wait()
		if rf.state == Leader {
			for i := range rf.peers {
				if i != rf.me {
					prevLogIndex := rf.nextIndex[i] - 1
					prevLogPos := prevLogIndex - rf.log[0].Index

					if prevLogPos < 0 {
						data := make([]byte, len(rf.snapshot))
						copy(data, rf.snapshot)
						args := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: rf.snapshotIndex, LastIncludedTerm: rf.snapshotTerm, Data: data}
						DPrintf(rf.logger, "Start Send InstallSnapshot Request to server %v:\n\t%v\n\t%v", i, args.Format(), rf.Format())
						go rf.sendInstallSnapshot(i, args)
					} else {
						entries := make([]LogEntry, len(rf.log[prevLogPos + 1:]))
						copy(entries, rf.log[prevLogPos + 1:])
						prevLogTerm := rf.log[prevLogPos].Term
						args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: entries, LeaderCommit: rf.commitIndex}
						DPrintf(rf.logger, "Start Send Heartbeats(AppendRequest) to server %v:\n\t%v\n\t%v", i, args.Format(), rf.Format())
						go rf.sendAppendEntries(i, args)
					}
				}
			}

			for logPos := len(rf.log) - 1; rf.log[logPos].Term == rf.currentTerm && rf.log[logPos].Index > rf.commitIndex; logPos-- {
				cnt := 1
				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= rf.log[logPos].Index {
						cnt += 1
					}
				}

				if cnt > len(rf.peers) / 2 {
					rf.commitIndex = rf.log[logPos].Index
					break
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.snapshotIndex && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		applyMsgs := make([]ApplyMsg, 0)
		if rf.lastApplied < rf.snapshotIndex {
			rf.lastApplied = rf.snapshotIndex
			data := make([]byte, len(rf.snapshot))
			copy(data, rf.snapshot)
			applyMsg := ApplyMsg{SnapshotValid: true, Snapshot: data, SnapshotTerm: rf.snapshotTerm, SnapshotIndex: rf.snapshotIndex}
			applyMsgs = append(applyMsgs, applyMsg)
		}

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			applyMsg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied - rf.log[0].Index].Command, CommandIndex: rf.lastApplied}
			applyMsgs = append(applyMsgs, applyMsg)
		}
		rf.mu.Unlock()

		for _, applyMsg := range applyMsgs {
			rf.applyCh <- applyMsg
			DPrintf(rf.logger, "Apply Log:\n\t%v", applyMsg.Format())
		}
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
	rf.voteCnt = 0
	rf.applyCh = applyCh
	rf.heartbeatCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.timer = time.Now()
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.snapshotIndex = 0
	rf.snapshotTerm = 0
	rf.snapshot = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	
	// Debug log
	rf.initDebug()

	// start ticker goroutine to start elections
	go rf.ticker()

	// start heartBeater goroutine to start heartbeats (only leader do)
	go rf.heartBeater()
	
	// periodically apply logs/snapshots to state machine
	go rf.applier()

	return rf
}
