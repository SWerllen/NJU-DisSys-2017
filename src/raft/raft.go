package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new PrintLog entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the PrintLog, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)
import "../labrpc"

// import "bytes"
// import "encoding/gob"

type (
	Role string
)

const (
	FOLLOWER             Role = "Follower"
	CANDIDATE            Role = "Candidate"
	LEADER               Role = "Leader"
	HeartBeatDuration    int  = 50
	NoOneChoose          int  = -1
	Diff_Duration_Server int  = 300
)

//
// as each Raft peer becomes aware that successive PrintLog entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu           sync.Mutex
	peers        []*labrpc.ClientEnd
	persister    *Persister
	me           int // index into peers[]
	stateMachine chan ApplyMsg

	// Your data here.
	Ticker        *time.Ticker
	LeaderTicker  *time.Ticker // 管理领导者定时心跳包的ticker
	ctxCancelFunc *context.CancelFunc

	Role        Role
	CurrentItem int
	VotedFor    int
	Log         []Log

	CommitIndex int
	LastApplied int

	// Leaders
	NextIndex  []int
	MatchIndex []int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = int(rf.CurrentItem)
	isleader = (rf.Role == LEADER)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	//w := new(bytes.Buffer)
	//e := gob.NewEncoder(w)
	//e.Encode(rf.xxx)
	//e.Encode(rf.yyy)
	//data := w.Bytes()
	//rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// AppendEntries
//
type AppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

//
// AppendEntriesReply
//
type AppendEntriesReply struct {
	Term    int
	success bool
}

//
// Log Entity
//
type Log struct {
	Term    int
	Command interface{}
}

//
// Normal Handler to RPC Message
//
func (rf *Raft) NormalHandler(messageTerm int) {
	if rf.CurrentItem < messageTerm {
		rf.CurrentItem = messageTerm
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("%d 收到来自 %d 的竞选消息\n", rf.me, args.CandidateId)

	index := rf.CommitIndex
	log := rf.Log[index]

	canVote := false
	rf.mu.Lock()
	reply.Term = rf.CurrentItem
	if args.Term > rf.CurrentItem {
		// 如果竞选任期增加了，之前投的票也不算
		rf.VotedFor = NoOneChoose
	}
	if (rf.Role == LEADER) ||
		(args.Term <= rf.CurrentItem) ||
		(rf.VotedFor != NoOneChoose && rf.VotedFor != args.CandidateId) ||
		(log.Term > args.LastLogTerm) ||
		(log.Term == args.LastLogTerm && index > args.LastLogIndex) {
		fmt.Println(args, rf.VotedFor, rf.CurrentItem, log.Term, index)
		DPrintf("%t, %t, %t, %t, %t", (rf.Role == LEADER), (args.LastLogTerm < rf.CurrentItem), (rf.VotedFor != NoOneChoose && rf.VotedFor != args.CandidateId),
			(log.Term > args.LastLogTerm), (log.Term == args.LastLogTerm && index > args.LastLogIndex))
		canVote = false
	} else {
		rf.VotedFor = args.CandidateId
		canVote = true
	}

	rf.NormalHandler(args.Term)
	rf.mu.Unlock()
	reply.VoteGranted = canVote
	return
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
func (rf *Raft) SendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) MakeRequestVote() RequestVoteArgs {
	lastIndex := len(rf.Log) - 1
	var term = 0
	if lastIndex > 0 {
		term = rf.Log[lastIndex].Term
	}
	return RequestVoteArgs{
		Term:         rf.CurrentItem,
		CandidateId:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  term,
	}
}

//
// 处理添加日志消息
//
func (rf *Raft) AppendEntries(args AppendEntries, reply *AppendEntriesReply) {
	// 当 发送者和接收者之间断开的时候，还是会调用该函数，不过rf不是之前的rf了，那Call函数返回什么bool值？？

	//DPrintf("%d 收到来自 %d 的过期消息，地址：%p\n", rf.me, args.LeaderId, rf)

	reply.Term = rf.CurrentItem
	if args.Term < rf.CurrentItem {
		DPrintf("%p %d 收到来自 %d 的过期消息\n", rf, rf.me, args.LeaderId)
		reply.success = false
		return
	}

	if rf.Role == LEADER {
		DPrintf("%d[%d] 领导时收到其他领导者 %d[%d] 的Append消息", rf.me, rf.CurrentItem, args.LeaderId, args.Term)
	}
	rf.mu.Lock()
	rf.VotedFor = NoOneChoose
	rf.Role = FOLLOWER
	rf.ResetRunVoteTicker()
	if rf.ctxCancelFunc != nil {
		DPrintf("%d 正在参加竞选活动时接收 %d Appen消息，竞选被取消", rf.me, args.LeaderId)
		(*rf.ctxCancelFunc)()
		rf.ctxCancelFunc = nil
	}
	if rf.LeaderTicker != nil {
		rf.LeaderTicker.Stop()
	}
	rf.NormalHandler(args.Term)
	rf.mu.Unlock()

	if len(args.Entries) == 0 {
		//DPrintf("%p %d 收到来自 %d 的心跳包消息\n", rf, rf.me, args.LeaderId)
	} else {
		//DPrintf("%p %d 收到来自 %d 的日志包消息\n", rf, rf.me, args.LeaderId)
	}
}

//
// 发送添加日志消息
//
func (rf *Raft) SendAppend(server int, args AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//DPrintf("%d 发送Appen消息，成功：%t", rf.me, ok)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's PrintLog. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft PrintLog, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := (rf.Role == LEADER)

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

func (rf *Raft) Apply(mes ApplyMsg) {
	rf.stateMachine <- mes
}

func (rf *Raft) ResetRunVoteTicker() {
	if rf.Ticker == nil {
		rf.Ticker = time.NewTicker(999999)
	}
	rand.Seed(time.Now().Unix())
	duration := time.Duration(int(time.Millisecond) * (1 + rf.me) * Diff_Duration_Server)
	rf.Ticker.Stop()
	rf.Ticker.Reset(duration)
}
func (rf *Raft) StopTicker() bool {
	if rf.Ticker == nil {
		return false
	}
	rf.Ticker.Stop()
	return true
}

func (rf *Raft) SendRequestVoteALl() bool {
	request := rf.MakeRequestVote()
	c := make(chan RequestVoteReply)
	for id, _ := range rf.peers {
		if id != rf.me {
			go func(c chan RequestVoteReply, request *RequestVoteArgs, id int) {
				reply := RequestVoteReply{}
				rf.SendRequestVote(id, *request, &reply)
				if reply.VoteGranted {
					DPrintf("	%d 投票给 %d\n", id, rf.me)
				}
				c <- reply
			}(c, &request, id)
		}
	}
	votedCount := 1
	success := false
	for i := 0; i < len(rf.peers)-1; i++ {
		reply := <-c
		if reply.VoteGranted {
			votedCount++
			success = (votedCount > int(math.Floor(float64(len(rf.peers)/2))))
			if success {
				return true
			}
		}
	}
	return false
}
func (rf *Raft) SendAppendAll(item AppendEntries) bool {
	DPrintf("%d 发送Append包", rf.me)
	c := make(chan AppendEntriesReply)
	for id, _ := range rf.peers {
		if id != rf.me {
			go func(c chan AppendEntriesReply, rf *Raft, id int) {
				if rf.me == 0 {
					DPrintf("%d[%d] 发送Append包给 %d[%d]", rf.me, rf.CurrentItem, id, -1)
				}
				reply := AppendEntriesReply{}
				ok := rf.SendAppend(id, item, &reply)
				if rf.me == 0 {
					DPrintf("%d[%d] 回复Append包给 %d[%d]", id, reply.Term, rf.me, rf.CurrentItem)
				}
				if !ok {
					c <- AppendEntriesReply{}
				} else {
					c <- reply
				}
			}(c, rf, id)
		}
	}

	var reply AppendEntriesReply
	for i := 0; i < len(rf.peers)-1; i++ {
		select {
		case reply = <-c:
			if !reply.success && reply.Term > rf.CurrentItem {
				DPrintf("%d 领导时发现有更新消息，退化为Follower", rf.me)
				rf.mu.Lock()
				rf.Role = FOLLOWER
				if rf.LeaderTicker != nil {
					rf.LeaderTicker.Stop()
				}
				rf.VotedFor = NoOneChoose
				rf.mu.Unlock()
			}
			break
		case <-time.After(time.Millisecond * 200):
			if rf.me == 1 {
				DPrintf("%d[%d] 发送的Append消息超时了！", rf.me, rf.CurrentItem)
			}
			break
		}
	}
	return true
}

func (rf *Raft) Lead() {
	if rf.LeaderTicker == nil {
		rf.LeaderTicker = time.NewTicker(time.Duration(int(time.Millisecond) * HeartBeatDuration))
	} else {
		rf.LeaderTicker.Reset(time.Duration(int(time.Millisecond) * HeartBeatDuration))
	}
	item := AppendEntries{
		Term:     rf.CurrentItem,
		LeaderId: rf.me,
	}
	res := rf.SendAppendAll(item)
	if !res {
		DPrintf("%d 下台，发送心跳包时发现有更高任期", rf.me)
		return
	}
	for {
		<-rf.LeaderTicker.C
		if rf.Role != LEADER {
			return
		}
		res = rf.SendAppendAll(item)
		if !res {
			DPrintf("%d 发送心跳包时发现有更高任期", rf.me)
			return
		}
	}
}

func (rf *Raft) RunVote(ctx context.Context, ticker *time.Ticker) bool {
	select {
	case <-ctx.Done():
		DPrintf("%d 中途停止竞选\n", rf.me)
		break
	case <-ticker.C:
		DPrintf("%d 时间超时，中途停止竞选\n", rf.me)
		break
	default:
		DPrintf("%d 开始发票\n", rf.me)
		rf.mu.Lock()
		rf.Role = CANDIDATE
		rf.VotedFor = rf.me
		rf.mu.Unlock()
		success := rf.SendRequestVoteALl()
		if success {
			rf.mu.Lock()
			rf.Role = LEADER
			rf.VotedFor = NoOneChoose
			rf.StopTicker()
			DPrintf("%d 竞选成功！\n", rf.me)
			rf.mu.Unlock()
			return true
		} else {
			DPrintf("%d 竞选失败，重新竞选！\n", rf.me)
		}
	}
	return false
}

func WaitForRunVote(rf *Raft) {
	rf.ResetRunVoteTicker()
	for {
		<-rf.Ticker.C
		rf.ResetRunVoteTicker()
		if rf.Role == LEADER {
			continue
		}
		go func(rf *Raft) {
			rf.CurrentItem = rf.CurrentItem + 1
			DPrintf("%d 开始竞选第 %d 任，地址：%p\n", rf.me, rf.CurrentItem, rf)
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			rf.mu.Lock()
			if rf.ctxCancelFunc != nil {
				rf.ctxCancelFunc = nil
			}
			rf.ctxCancelFunc = &cancel
			rf.mu.Unlock()
			DPrintf("%d 取消函数的地址：%p", rf.me, rf.ctxCancelFunc)

			res := rf.RunVote(ctx, rf.Ticker)

			rf.mu.Lock()
			if rf.ctxCancelFunc != nil {
				rf.ctxCancelFunc = nil
			}
			rf.mu.Unlock()

			if res {
				rf.Lead()
			}
		}(rf)
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
	rf.mu = sync.Mutex{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.stateMachine = applyCh
	rf.Role = FOLLOWER
	rf.CurrentItem = 0
	rf.VotedFor = NoOneChoose
	rf.Log = []Log{{}}

	rf.CommitIndex = 0
	rf.LastApplied = 0

	rf.MatchIndex = make([]int, len(peers))
	rf.NextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.NextIndex[i] = 0
		rf.MatchIndex[i] = 0
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go WaitForRunVote(rf)

	return rf
}
