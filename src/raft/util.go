package raft

import "log"
import "os"
import "fmt"
import "time"

func max(a int, b int) int {
    if a > b {
        return a
    }
    return b
}

func min(a int, b int) int {
    if a < b {
        return a
    }
    return b
}

// Debugging
const Debug = false

func (rf *Raft) initDebug() {
    if Debug {
        if rf.logger == nil {
            filename := fmt.Sprintf("server%d.log", rf.me)
            logfile, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
            if err!= nil {
                panic(fmt.Sprintf("can't open %v server logger file", rf.me))
            }
            rf.logger = log.New(logfile, "", 0)
        }
	}
} 

func DPrintf(logger *log.Logger, format string, a ...interface{}) {
	if Debug {
        now := time.Now()
        prefix := fmt.Sprintf("[%02d:%02d:%02d.%03d]",
            now.Hour(), now.Minute(), now.Second(), now.Nanosecond()/1000000)
        logger.Printf("%s %s", prefix, fmt.Sprintf(format, a...))
	}
}

type Formater interface {
	Format() string
}

func (msg *ApplyMsg) Format() string {
    commandStr := "nil"
    if msg.CommandValid {
        commandStr = fmt.Sprintf("%v", msg.Command)
    }
    return fmt.Sprintf("ApplyMsg{CommandValid: %t, Command: %s, CommandIndex: %d, SnapshotValid: %t, SnapshotTerm: %d, SnapshotIndex: %d}", msg.CommandValid, commandStr, msg.CommandIndex, msg.SnapshotValid, msg.SnapshotTerm, msg.SnapshotIndex)
}

func (rf *Raft) Format() string {
	logStr := "[]"
    // if len(rf.log) > 0 {
    //    logStr = "["
    //    for _, entry := range rf.log {
    //       logStr += fmt.Sprintf("%v, ", entry)
    //    }
    //    logStr = logStr[:len(logStr)-2] + "]"
    // }

    nextIndexStr := "[]"
    if len(rf.nextIndex) > 0 {
       nextIndexStr = "["
       for _, index := range rf.nextIndex {
          nextIndexStr += fmt.Sprintf("%d, ", index)
       }
       nextIndexStr = nextIndexStr[:len(nextIndexStr)-2] + "]"
    }

    matchIndexStr := "[]"
    if len(rf.matchIndex) > 0 {
       matchIndexStr = "["
       for _, index := range rf.matchIndex {
          matchIndexStr += fmt.Sprintf("%d, ", index)
       }
       matchIndexStr = matchIndexStr[:len(matchIndexStr)-2] + "]"
    }
    
    return fmt.Sprintf("Raft{state: %d, currentTerm: %d, voteFor: %d, log: %s, commitIndex: %d, lastApplied: %d, snapshotIndex: %d, snapshotTerm: %d, nextIndex: %s, matchIndex: %s}", rf.state, rf.currentTerm, rf.voteFor, logStr, rf.commitIndex, rf.lastApplied, rf.snapshotIndex, rf.snapshotTerm, nextIndexStr, matchIndexStr)
}

func (args *RequestVoteArgs) Format() string {
    return fmt.Sprintf("RequestVoteArgs{Term: %d, CandidateId: %d, LastLogIndex: %d, LastLogTerm: %d}", args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
}

func (reply *RequestVoteReply) Format() string {
    return fmt.Sprintf("RequestVoteReply{Term: %d, VoteGranted: %t}", reply.Term, reply.VoteGranted)
}

func (args *AppendEntriesArgs) Format() string {
    entriesStr := "[]"
	if len(args.Entries) > 0 {
		entriesStr = "["
		for _, entry := range args.Entries {
			entriesStr += fmt.Sprintf("%v, ", entry)
		}
		entriesStr = entriesStr[:len(entriesStr)-2] + "]"
	}

    return fmt.Sprintf("AppendEntriesArgs{Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, Entries: %s, LeaderCommit: %d}", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, entriesStr, args.LeaderCommit)
}

func (reply *AppendEntriesReply) Format() string {
    return fmt.Sprintf("AppendEntriesReply{Term: %d, Success: %t}", reply.Term, reply.Success)
}

func (args *InstallSnapshotArgs) Format() string {
    return fmt.Sprintf("InstallSnapshotArgs{Term: %d, LeaderId: %d, LastIncludedIndex: %d, LastIncludedTerm: %d}", args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
}

func (reply *InstallSnapshotReply) Format() string {
    return fmt.Sprintf("InstallSnapshotReply{Term: %d}", reply.Term)
}
