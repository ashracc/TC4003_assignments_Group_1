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
	// "fmt"
	"sync"
	"labrpc"
	"bytes"
	"encoding/gob"
	"time"
	"math/rand"
)

const (
	STATE_LEADER = 0
	STATE_CANDIDATE = 1
	STATE_FLLOWER = 2
	
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	LogTerm int
	LogComd interface{}
	LogIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//channel
	state int
	voteAcquired int
	heartbeatCh chan interface{}
	leaderCh chan interface{}
	commitCh chan interface{}
	grantedvoteCh chan interface{}
	applyCh chan ApplyMsg

	//persistent state on all server
	currentTerm int
	votedFor int
	log []LogEntry

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on leader
	nextIndex []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.state == STATE_LEADER
	return term, isleader
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log) - 1].LogIndex
}
func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log) - 1].LogTerm
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
 	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)	// As per Figure 2 RAFT's paper
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readSnapshot(data []byte) {
	rf.readPersist(rf.persister.ReadRaftState())
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var LastIncludedIndex int
	var LastIncludedTerm int
	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)
	rf.commitIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex
	rf.log = deleteLog(LastIncludedIndex, LastIncludedTerm, rf.log)
	msg := ApplyMsg {
		UseSnapshot: 	true,
		Snapshot:		data,
	}
	go func() {
		rf.applyCh <- msg
	}()
}
//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}
//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here.
	Term int
	LeaderId int
	PrevLogTerm int
	PrevLogIndex int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here.
	Term int
	Success bool
	NextIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
	// your code here
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].LogIndex
	lastIndex := rf.getLastIndex()

	if index <= baseIndex || index > lastIndex {
		return
	}

	var newLogEntries []LogEntry
	logentry := LogEntry {
		LogIndex: index,
		LogTerm: rf.log[index-baseIndex].LogTerm,
	}
	newLogEntries = append(newLogEntries, logentry)
	for i := index+1; i <= lastIndex; i++ {
		newLogEntries = append(newLogEntries, rf.log[i-baseIndex])
	}

	rf.log = newLogEntries
	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLogEntries[0].LogIndex)	
	e.Encode(newLogEntries[0].LogTerm)	

	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false
	//  If the candidate’s term is smaller than his own, he will not vote for the candidate.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	term := rf.getLastTerm()
	index := rf.getLastIndex()

	uptoDate := false
	if args.LastLogTerm > term {
		uptoDate = true
	}

	if args.LastLogTerm == term && args.LastLogIndex >= index { // at least up to date
		uptoDate = true
	}

	// If you have already voted
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptoDate {
		rf.grantedvoteCh <- struct{}{}
		rf.state = STATE_FLLOWER
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	// Heartbeat
	rf.heartbeatCh <- struct{}{}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FLLOWER
		rf.votedFor = -1
	}
	reply.Term = args.Term

	if args.PrevLogIndex > rf.getLastIndex() {
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	// take the index of the log in the first position as the reference point
	baseIndex := rf.log[0].LogIndex

	if args.PrevLogIndex > baseIndex {
		term := rf.log[args.PrevLogIndex-baseIndex].LogTerm
		if args.PrevLogTerm != term {
			for i := args.PrevLogIndex - 1 ; i >= baseIndex; i-- {
				if rf.log[i-baseIndex].LogTerm != term {
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}
	}

	if args.PrevLogIndex >= baseIndex {
		rf.log = rf.log[: args.PrevLogIndex+1 - baseIndex]
		rf.log = append(rf.log, args.Entries...)

		reply.Success = true
		reply.NextIndex = rf.getLastIndex() + 1
	}	

	if args.LeaderCommit > rf.commitIndex {
		last := rf.getLastIndex()
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.commitCh <- struct{}{}
	}
	return
}

func deleteLog(lastIncludedIndex int, lastIncludedTerm int, log []LogEntry) []LogEntry {
	var newLogEntries []LogEntry
	logentry := LogEntry {
		LogIndex: lastIncludedIndex,
		LogTerm: lastIncludedTerm,
	}
	newLogEntries = append(newLogEntries, logentry)

	for i := len(log) - 1; i >= 0; i-- {
		if log[i].LogIndex == lastIncludedIndex && log[i].LogTerm == lastIncludedTerm {
			newLogEntries = append(newLogEntries, log[i+1:]...)
			break
		}
	}

	return newLogEntries
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// As with the transfer log, also with a heartbeat
	rf.heartbeatCh <- struct{}{}
	rf.persister.SaveSnapshot(args.Data)
	rf.log = deleteLog(args.LastIncludedIndex, args.LastIncludedTerm, rf.log)

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.persist()

	msg := ApplyMsg {
		UseSnapshot: 	true,
		Snapshot:		args.Data,
	}
	// Return data to the upper server
	rf.applyCh <- msg
}


type InstallSnapshotArgs struct {
	Term 		int 
	LeaderId 	int
	LastIncludedIndex	int
	LastIncludedTerm 	int
	Data	[]byte
}

type InstallSnapshotReply struct {
	Term 	int
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	// call RPC
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		term := rf.currentTerm
		if rf.state != STATE_CANDIDATE {
			return ok
		}
		if args.Term != term {
			return ok
		}

		// update your own term
		if reply.Term > term {
			rf.currentTerm = reply.Term
			rf.state = STATE_FLLOWER
			rf.votedFor = -1
			rf.persist()
		}
		if reply.VoteGranted {
			rf.voteAcquired++
			if rf.state == STATE_CANDIDATE && rf.voteAcquired > len(rf.peers)/2 {
				rf.state = STATE_FLLOWER
				rf.leaderCh <- struct{}{}
			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != STATE_LEADER {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}

		// update your own term
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = STATE_FLLOWER
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].LogIndex + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.nextIndex[server] = reply.NextIndex
		}
	}
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	if ok {
		// update your own term
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = STATE_FLLOWER
			rf.votedFor = -1
			// rf.persist()
			return ok
		}

		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}

	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == STATE_LEADER

	if isLeader {
		index = rf.getLastIndex() + 1
		rf.log = append(rf.log, LogEntry{LogTerm:term, LogComd:command, LogIndex:index}) // append new entry from client
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) boatcastRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()
	// Initialize the function parameters to be passed to the RPC call
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogTerm = rf.getLastTerm()
	args.LastLogIndex = rf.getLastIndex()
	rf.mu.Unlock()

	// Traverse the entire Raft server array and perform leader elections
	for i := range rf.peers {
		if i != rf.me && rf.state == STATE_CANDIDATE {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, args, &reply)
			}(i)
		}
	}
}

func (rf *Raft) boatcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	N := rf.commitIndex
	last := rf.getLastIndex()
	baseIndex := rf.log[0].LogIndex
	for i := rf.commitIndex + 1; i <= last; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i-baseIndex].LogTerm == rf.currentTerm {
				// fmt.Printf("who agree = %v\n", j)
				num++
			}
		}
		if 2*num > len(rf.peers) {
			N = i
			// fmt.Printf("rf.me = %v, count = %v, len(peers) = %v\n", rf.me, count, len(rf.peers))
		}
	}
	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.commitCh <- true
	}

	for i := range rf.peers {
		if i != rf.me && rf.state == STATE_LEADER {
			if rf.nextIndex[i] > baseIndex {
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].LogTerm
				args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex + 1 - baseIndex:]))
				copy(args.Entries, rf.log[args.PrevLogIndex + 1 - baseIndex:])
				args.LeaderCommit = rf.commitIndex
				go func(i int,args AppendEntriesArgs) {
					var reply AppendEntriesReply
					rf.sendAppendEntries(i, args, &reply)
				}(i,args)
			} else {
				// The follower is too far behind, can't send logs to him, send a snapshot to him
				var args InstallSnapshotArgs
				// parameter initialization
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.log[0].LogIndex
				args.LastIncludedTerm = rf.log[0].LogTerm
				args.Data = rf.persister.snapshot

				go func(i int, args InstallSnapshotArgs) {
					var reply InstallSnapshotReply
					rf.sendInstallSnapshot(i, args, &reply)
				}(i, args)
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = STATE_FLLOWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{LogTerm: 0})
	rf.currentTerm = 0
	rf.heartbeatCh = make(chan interface{}, 100)
	rf.leaderCh = make(chan interface{}, 100)
	rf.commitCh = make(chan interface{}, 100)
	rf.grantedvoteCh = make(chan interface{}, 100)

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// State machine loop
	go func() {
		for {
			switch rf.state {
			case STATE_FLLOWER:
				select {
				// Re-clocked after receiving a heartbeat or voting confirmation
				case <-rf.heartbeatCh:
				case <-rf.grantedvoteCh:
				// Timeout becomes a candidate
				case <-time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
					rf.state = STATE_CANDIDATE
				}
			case STATE_LEADER:
				// If it is a leader, perform the operation of copying the log (with a heartbeat)
				rf.boatcastAppendEntries()
				time.Sleep(50 * time.Millisecond)
			case STATE_CANDIDATE:
				// Candidates are prepared to vote
				rf.mu.Lock()
				rf.currentTerm++
				// Give yourself a vote first
				rf.votedFor = rf.me
				rf.voteAcquired = 1
				rf.persist()
				rf.mu.Unlock()

				go rf.boatcastRequestVote()

				select {
				case <-time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
				// Receive a heartbeat (other nodes become leaders first) candidates become followers
				case <-rf.heartbeatCh:
					rf.state = STATE_FLLOWER
				// Received a message from the leaderCh channel to win the election and become a leader
				case <-rf.leaderCh:
					rf.mu.Lock()
					rf.state = STATE_LEADER
					// fmt.Printf("leader = %v currentTerm = %v\n", rf.me, rf.currentTerm)

					// Initialize these two arrays for only the leader
					rf.nextIndex = make([]int,len(rf.peers))
					rf.matchIndex = make([]int,len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				}
			}
		}
	}()

	go func() {
		for {
			select {
			// 日志提交
			case <-rf.commitCh:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				baseIndex := rf.log[0].LogIndex
				for i := rf.lastApplied+1; i <= commitIndex; i++ {
					msg := ApplyMsg{Index: i, Command: rf.log[i-baseIndex].LogComd}
					// fmt.Printf("leader = %v, me = %v, msg.Index = %v, msg.Command = %v\n", rf.state==STATE_LEADER, rf.me, msg.Index, msg.Command)
					applyCh <- msg
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()
	return rf
}