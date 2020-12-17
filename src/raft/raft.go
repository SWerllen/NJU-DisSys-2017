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
	StateMachine *chan ApplyMsg

	// Your data here.
	Ticker        *time.Ticker
	LeaderTicker  *time.Ticker // 管理领导者定时心跳包的ticker
	ctxCancelFunc *context.CancelFunc

	Role        Role
	CurrentTerm int
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
	term = int(rf.CurrentTerm)
	isleader = (rf.Role == LEADER)

	return term, isleader
}

func (rf *Raft) GetLastIndex() int {
	return len(rf.Log) - 1
}

func (rf *Raft) AppendLog(isLocked bool, logs ...Log) {
	if !isLocked {
		rf.mu.Lock()
	}
	rf.Log = append(rf.Log, logs...)
	if !isLocked {
		rf.mu.Unlock()
	}
}

func (rf *Raft) SliceLog(isLocked bool, end int) {
	if !isLocked {
		rf.mu.Lock()
	}
	rf.Log = rf.Log[0:end]
	if !isLocked {
		rf.mu.Unlock()
	}
}

func (rf *Raft) SetCommitIndex(isLocked bool, index int) int {
	if !isLocked {
		rf.mu.Lock()
	}
	if rf.CommitIndex < index && rf.Log[index].Term == rf.CurrentTerm {
		rf.CommitIndex = index
		if !isLocked {
			rf.mu.Unlock()
		}
		return index
	}
	if !isLocked {
		rf.mu.Unlock()
	}
	return rf.CommitIndex
}

func (rf *Raft) UpdateMatchIndex(id int, index int) {
	rf.mu.Lock()
	if rf.MatchIndex[id] < index {
		rf.MatchIndex[id] = index
	}
	rf.mu.Unlock()
}

func (rf *Raft) SetNextIndex(id int, index int) {
	rf.mu.Lock()
	if rf.NextIndex[id] < index {
		rf.NextIndex[id] = index
	}
	rf.mu.Unlock()
}

func (rf *Raft) DecrementNextIndex(id int, preIndex int) {
	rf.mu.Lock()
	if rf.NextIndex[id] >= preIndex {
		rf.NextIndex[id] = preIndex - 1
	}
	rf.mu.Unlock()
}

func (rf *Raft) GetDeepCopy() Raft {
	return Raft{
		peers:         rf.peers,
		persister:     rf.persister,
		me:            rf.me,
		StateMachine:  rf.StateMachine,
		LeaderTicker:  rf.LeaderTicker,
		ctxCancelFunc: rf.ctxCancelFunc,
		Role:          rf.Role,
		CurrentTerm:   rf.CurrentTerm,
		VotedFor:      rf.VotedFor,
		Log:           append(make([]Log, 0), rf.Log...),
		CommitIndex:   rf.CommitIndex,
		LastApplied:   rf.LastApplied,
		NextIndex:     append(make([]int, 0), rf.NextIndex...),
		MatchIndex:    append(make([]int, 0), rf.MatchIndex...),
	}
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
// AppendEntriesArgs
//
type AppendEntriesArgs struct {
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
	Success bool
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
	if rf.CurrentTerm < messageTerm {
		rf.CurrentTerm = messageTerm
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("%d[%d] 收到来自 %d[%d] 的竞选消息\n", rf.me, rf.CurrentTerm, args.CandidateId, args.Term)

	index := rf.CommitIndex
	log := rf.Log[index]

	canVote := false
	reply.Term = rf.CurrentTerm

	if rf.Role == LEADER && args.Term > rf.CurrentTerm {
		rf.mu.Lock()
		rf.Role = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VotedFor = NoOneChoose
		if rf.LeaderTicker != nil {
			rf.LeaderTicker.Stop()
		}
		rf.mu.Unlock()
	} else if args.Term > rf.CurrentTerm {
		rf.mu.Lock()
		// 如果竞选任期增加了，之前投的票也不算
		rf.VotedFor = NoOneChoose
		rf.CurrentTerm = args.Term
		if rf.ctxCancelFunc != nil && (*rf.ctxCancelFunc) != nil {
			(*rf.ctxCancelFunc)()
		}
		rf.mu.Unlock()
	}
	if (rf.Role == LEADER) ||
		(args.Term < rf.CurrentTerm) ||
		(rf.VotedFor != NoOneChoose && rf.VotedFor != args.CandidateId) ||
		(log.Term > args.LastLogTerm) ||
		(log.Term == args.LastLogTerm && index > args.LastLogIndex) {
		//fmt.Println(args, rf.VotedFor, rf.CurrentTerm, log.Term, index)
		DPrintf("%t, %t, %t, %t, %t", (rf.Role == LEADER), (args.LastLogTerm < rf.CurrentTerm), (rf.VotedFor != NoOneChoose && rf.VotedFor != args.CandidateId),
			(log.Term > args.LastLogTerm), (log.Term == args.LastLogTerm && index > args.LastLogIndex))
		canVote = false
	} else {
		rf.mu.Lock()
		rf.VotedFor = args.CandidateId
		canVote = true
		rf.ResetRunVoteTicker()
		rf.mu.Unlock()
	}

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
	lastIndex := rf.GetLastIndex()
	var term = 0
	if lastIndex > 0 {
		term = rf.Log[lastIndex].Term
	}
	return RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  term,
	}
}

//
// 处理添加日志消息
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// 当 发送者和接收者之间断开的时候，还是会调用该函数，不过rf不是之前的rf了，那Call函数返回什么bool值？？

	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		// 这里有个问题，当一个follower节点自闭久了自己把任期增加到很高，然后重新连接到集群后，就不会服从leader的管教，就会不停地竞选
		//if len(args.Entries) > 0 && args.PrevLogTerm > rf.Log[rf.GetLastIndex()].Term {
		//	// 如果是由日志term的强行压制
		//	rf.mu.Lock()
		//	rf.Role = FOLLOWER
		//	rf.CurrentTerm = args.Term
		//	rf.StopTicker()
		//	rf.mu.Unlock()
		//}
		reply.Success = false
		return
	}

	if rf.Role == LEADER {
		DPrintf("%d[%d] 领导时收到其他领导者 %d[%d] 的Append消息", rf.me, rf.CurrentTerm, args.LeaderId, args.Term)
	}
	rf.mu.Lock()
	rf.VotedFor = NoOneChoose
	rf.Role = FOLLOWER
	rf.ResetRunVoteTicker()
	if rf.ctxCancelFunc != nil {
		DPrintf("%d[%d] 正在参加竞选活动时接收 %d[%d] Appen消息，竞选被取消", rf.me, rf.CurrentTerm, args.LeaderId, args.Term)
		(*rf.ctxCancelFunc)()
		rf.ctxCancelFunc = nil
	}
	if rf.LeaderTicker != nil {
		rf.LeaderTicker.Stop()
	}
	rf.NormalHandler(args.Term)
	rf.mu.Unlock()

	if len(args.Entries) != 0 {
		DPrintf("%d[originLen: %d] 收到来自 %d[%d] 的日志包消息 [PrevIndex: %d；len：%d]\n", rf.me, len(rf.Log),
			args.LeaderId, args.Term, args.PrevLogIndex, len(args.Entries))
		if args.PrevLogIndex > rf.GetLastIndex() {
			DPrintf("%d[%d] 日志缺少PreLogIndex位置记录\n", rf.me, rf.CurrentTerm)
			reply.Success = false
		} else if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
			DPrintf("%d[%d] 日志PreLogIndex位置记录任期与PrevLogTerm不符\n", rf.me, rf.CurrentTerm)
			rf.SliceLog(false, args.PrevLogIndex)
			reply.Success = false
			return
		} else {
			var appendStartIndex = 0
			for i, entity := range args.Entries {
				correspondingIndex := i + args.PrevLogIndex + 1
				if correspondingIndex > rf.GetLastIndex() {
					break
				}
				appendStartIndex = i + 1
				correspondingEntity := rf.Log[correspondingIndex]
				if entity.Command != correspondingEntity.Command {
					DPrintf("%d[%d] 日志Log第 %d 位置Command[%d]和Append消息序列第 %d 位置Command[%d]不一致\n",
						rf.me, rf.CurrentTerm, correspondingIndex, correspondingEntity.Command, i, entity.Command)
					rf.SliceLog(false, correspondingIndex)
					reply.Success = false
					return
				}
			}
			rf.AppendLog(false, args.Entries[appendStartIndex:len(args.Entries)]...)
			reply.Success = true
			DPrintf("%d[%d] 更新日志序列%v\n", rf.me, rf.CurrentTerm, rf.Log)
		}
	} else {
		//DPrintf("%d 收到来自 %d 的心跳包消息\n", rf.me, args.LeaderId)
		reply.Success = true
	}

	rf.mu.Lock()
	if args.LeaderCommit > rf.CommitIndex {
		oldCommitIndex := rf.CommitIndex
		newCommitIndex := rf.SetCommitIndex(true, int(math.Min(float64(args.LeaderCommit), float64(rf.GetLastIndex()))))
		//fmt.Println(oldCommitIndex, newCommitIndex)
		for i := oldCommitIndex + 1; i <= newCommitIndex; i++ {
			rf.Execute(i, rf.Log[i].Command)
		}
		rf.mu.Unlock()
	} else {
		rf.mu.Unlock()
	}

}

//
// 发送添加日志消息
//
func (rf *Raft) SendAppend(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

func (rf *Raft) Apply(mes ApplyMsg) {
	*(rf.StateMachine) <- mes
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
					DPrintf("%d[%d] 投票给 %d[%d]\n", id, reply.Term, rf.me, rf.CurrentTerm)
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

//
// 发送给所有的非己节点发送一样的包
//
func (rf *Raft) Heartbeat() bool {
	rf.mu.Lock()
	item := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.CommitIndex,
	}
	rf.mu.Unlock()

	//DPrintf("%d 发送心跳包", rf.me)
	for id, _ := range rf.peers {
		if id != rf.me {
			go func(rf *Raft, id int) {
				reply := AppendEntriesReply{}
				rf.SendAppend(id, item, &reply)
			}(rf, id)
		}
	}
	return true
}

//
// 向Followers发送日志添加的请求，如果超过半数接受，则commit
//
func (rf *Raft) PrepareCommit(singleLog Log) int {
	DPrintf("%d[%d] 发送Append包\n", rf.me, rf.CurrentTerm)
	c := make(chan AppendEntriesReply)
	rf.mu.Lock()
	rf.AppendLog(true, singleLog)
	insertIndex := rf.GetLastIndex()
	rf.mu.Unlock()
	tmpRf := rf.GetDeepCopy()
	for id, _ := range rf.peers {
		if id != rf.me {
			go func(c chan AppendEntriesReply, tmpRf *Raft, id int, log Log, realRf *Raft) {
				var startPrevIndex = tmpRf.GetLastIndex() - 1
				for true {
					prevLogIndex := int(math.Max(0, math.Min(float64(startPrevIndex), float64(tmpRf.NextIndex[id]))))

					if tmpRf.NextIndex[id] > tmpRf.GetLastIndex() || realRf.Role != LEADER {
						return
					}
					item := AppendEntriesArgs{
						Term:         tmpRf.CurrentTerm,
						LeaderId:     tmpRf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  tmpRf.Log[prevLogIndex].Term,
						Entries:      tmpRf.Log[prevLogIndex+1:],
						LeaderCommit: tmpRf.CommitIndex,
					}
					tmpReply := AppendEntriesReply{}
					ok := realRf.SendAppend(id, item, &tmpReply)
					//DPrintf("%d 收到 %d Append回复[%t]，消息内容为%t", rf.me, id, ok, tmpReply.Success)
					if ok && tmpReply.Success {
						realRf.UpdateMatchIndex(id, insertIndex)
						realRf.SetNextIndex(id, insertIndex)
						c <- tmpReply
						DPrintf("传入通道！")
						return
					} else if !ok {
						continue
					} else {
						tmpRf.DecrementNextIndex(id, prevLogIndex)
						if tmpReply.Term > tmpRf.CurrentTerm {
							c <- tmpReply
							DPrintf("%d[%d] 出现节点 %d[%d] 比自己任期大的情况！", tmpRf.me, tmpRf.CurrentTerm, id, tmpReply.Term)
							return
						}
					}
				}
			}(c, &tmpRf, id, singleLog, rf)
		}
	}

	var reply AppendEntriesReply
	trueCount := 1
	hasDown := false // 是否出现过任期没有别的节点任期大的情况
	maxTermHasSeen := rf.CurrentTerm
	for i := 0; i < len(rf.peers)-1; i++ {
		select {
		case reply = <-c:
			if reply.Success {
				trueCount++
				if trueCount > int(math.Floor(float64(len(rf.peers)/2))) {
					if insertIndex > rf.CommitIndex {
						rf.mu.Lock()
						oldCommitIndex := rf.CommitIndex
						newCommitIndex := rf.SetCommitIndex(true, insertIndex)
						//fmt.Println(oldCommitIndex, newCommitIndex)
						for i := oldCommitIndex + 1; i <= newCommitIndex; i++ {
							rf.Execute(i, rf.Log[i].Command)
						}
						rf.mu.Unlock()
					}
					DPrintf("%d[%d] 发送Append被超过一半的节点接受，位置：%d。 日志序列：%v", rf.me, rf.CurrentTerm, insertIndex, rf.Log)
					//close(c)
					return insertIndex
				}
			} else {
				// 当任期失效时，才会出现这个情况
				hasDown = true
				maxTermHasSeen = int(math.Min(float64(maxTermHasSeen), float64(reply.Term)))
			}
			break
		case <-time.After(time.Millisecond * 400):
			DPrintf("%d[%d] 发送Append超时，未满一半节点接受", rf.me, rf.CurrentTerm)
			if hasDown && rf.Role == LEADER {
				// 如果没有一半节点接受且已经出现别的节点任期比自己大的情况，退位
				rf.mu.Lock()
				rf.Role = FOLLOWER
				rf.CurrentTerm = maxTermHasSeen
				if rf.LeaderTicker != nil {
					rf.LeaderTicker.Stop()
				}
				rf.mu.Unlock()
			}
			//close(c)
			return insertIndex
		}
	}
	return insertIndex
}

func (rf *Raft) Lead() {
	if rf.LeaderTicker == nil {
		rf.LeaderTicker = time.NewTicker(time.Duration(int(time.Millisecond) * HeartBeatDuration))
	} else {
		rf.LeaderTicker.Reset(time.Duration(int(time.Millisecond) * HeartBeatDuration))
	}

	res := rf.Heartbeat()
	if !res {
		DPrintf("%d[%d] 下台，发送心跳包时发现有更高任期", rf.me, rf.CurrentTerm)
		return
	}
	for {
		<-rf.LeaderTicker.C
		if rf.Role != LEADER {
			return
		}
		res = rf.Heartbeat()
		if !res {
			DPrintf("%d[%d] 发送心跳包时发现有更高任期", rf.me, rf.CurrentTerm)
			return
		}
	}
}

func (rf *Raft) RunVote(ctx context.Context, ticker *time.Ticker) bool {
	select {
	case <-ctx.Done():
		DPrintf("%d[%d] 中途停止竞选\n", rf.me, rf.CurrentTerm)
		break
	case <-ticker.C:
		DPrintf("%d[%d] 时间超时，中途停止竞选\n", rf.me, rf.CurrentTerm)
		break
	default:
		DPrintf("%d[%d] 开始发票\n", rf.me, rf.CurrentTerm)
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
			DPrintf("%d[%d] 竞选成功！\n", rf.me, rf.CurrentTerm)
			lastIndex := rf.GetLastIndex()
			for id := 0; id < len(rf.peers); id++ {
				rf.NextIndex[id] = lastIndex + 1
				rf.MatchIndex[id] = 0
			}
			rf.mu.Unlock()
			return true
		} else {
			rf.mu.Lock()
			rf.Role = FOLLOWER
			rf.VotedFor = NoOneChoose
			rf.mu.Unlock()
			DPrintf("%d[%d] 竞选失败，重新竞选！\n", rf.me, rf.CurrentTerm)
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
			rf.CurrentTerm = rf.CurrentTerm + 1
			DPrintf("%d[%d] 开始竞选第 %d 任，地址：%p\n", rf.me, rf.CurrentTerm, rf.CurrentTerm, rf)
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			rf.mu.Lock()
			rf.ctxCancelFunc = &cancel
			rf.mu.Unlock()
			DPrintf("%d[%d] 取消函数的地址：%p", rf.me, rf.CurrentTerm, rf.ctxCancelFunc)

			res := rf.RunVote(ctx, rf.Ticker)

			if res {
				rf.Lead()
			}
		}(rf)
	}
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
	index := rf.GetLastIndex()
	term := rf.CurrentTerm
	isLeader := rf.Role == LEADER
	if !isLeader {
		return index, term, isLeader
	}
	DPrintf("客户传来command: %s， 接收者：%d[%d]", command, rf.me, rf.CurrentTerm)
	index = rf.PrepareCommit(Log{Command: command, Term: rf.CurrentTerm})

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
	rf.StopTicker()
	if rf.LeaderTicker != nil {
		rf.LeaderTicker.Stop()
	}
}

func (rf *Raft) Execute(index int, command interface{}) {
	*(rf.StateMachine) <- ApplyMsg{
		Index:       index,
		Command:     command,
		UseSnapshot: false,
		Snapshot:    nil,
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
	rf.StateMachine = &applyCh
	rf.Role = FOLLOWER
	rf.CurrentTerm = 0
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
