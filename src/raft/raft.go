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
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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

// serverRole
type ServerRole int

const (
	ROLE_Follwer   ServerRole = 1
	ROLE_Candidate ServerRole = 2
	ROLE_Leader    ServerRole = 3
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers 集群消息
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill ()是否死亡，1表示死亡，0表示还活着
	// 2A
	// state          NodeState   // 节点状态
	currentTerm    int // 当前任期
	votedFor       int // 给谁投过票
	votedCnt       int
	currentRole    ServerRole  // 当前role
	electionTimer  *time.Timer // 选举时间
	heartbeatTimer *time.Timer // 心跳时间
	heartbeatFlag  int         // follwer sleep 期间
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
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
	Term        int // candidate's term
	CandidateId int // candidate global only id
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // candidate's term
	CandidateId int  // candidate global only id
	VoteGranted bool // true 表示拿到票了
}

type AppendEntriesArgs struct {
	Term int
}

type AppendEntriesReply struct {
	Term int
}

/********** RPC  *************/

// 获取下次超时时间
func getRandomTimeout() time.Duration {
	// 300 ~ 450 ms 的误差
	return time.Duration(300+rand.Intn(150)) * time.Millisecond
}

// 获取当前时间
func getCurrentTime() int64 {
	return time.Now().UnixNano()
}

// 切换 role
func (rf *Raft) switchRole(role ServerRole) {
	// 如果相同直接return
	if rf.currentRole == role {
		return
	}
	rf.currentRole = role
	// 投票 重置为-1
	if role == ROLE_Follwer {
		rf.votedFor = -1
	}
	fmt.Printf("[SwitchRole] id=%d role=%d term=%d change to %d \n", rf.me, rf.currentRole, rf.currentTerm, role)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 		任期不对，先转化成follow
	if rf.currentTerm < args.Term {
		rf.switchRole(ROLE_Follwer)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	switch rf.currentRole {
	case ROLE_Follwer:
		// 先看这个follow有没有投票过
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	case ROLE_Candidate:
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
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
	// Your code here (2B).
	index := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.currentRole == ROLE_Leader
	term := rf.currentTerm
	if !isLeader {
		return index, term, isLeader
	}

	// record in local log
	//index = len(rf.log)
	//rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	//rf.SendAppendEntries() // for lab3a TestSpeed
	rf.heartbeatTimer.Reset(10 * time.Millisecond)
	rf.persist()
	//DPrintf("[Start] %s Add Log Index=%d Term=%d Command=%v\n", rf.role_info(), rf.getLogLogicSize(), rf.log[index].Term, rf.log[index].Command)

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

// leader发送心跳，检查任期号
func (rf *Raft) leaderHeartBeat() {

	for server, _ := range rf.peers {
		// 先排除自己
		if server == rf.me {
			continue
		}
		go func(s int) { // 给follow发心跳
			args := AppendEntriesArgs{}
			reply := AppendEntriesReply{}
			// 加一下锁
			rf.mu.Lock()
			args.Term = rf.currentTerm
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(s, &args, &reply)
			if !ok {
				fmt.Printf("[SendHeartbeat] id=%d send heartbeat to %d failed \n", rf.me, s)
				return
			}
			rf.mu.Lock()
			// leader收到回复的版本号比他自己还大，直接变follow
			if reply.Term > args.Term {
				rf.switchRole(ROLE_Follwer)
				rf.currentTerm = reply.Term
				// TODO rf.votedFor = -1
			}
			rf.mu.Unlock()
		}(server)
	}
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 发送心跳对应三个角色的执行
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 0.优先处理curterm<args.term,直接转化为follow
	if rf.currentTerm < args.Term {
		rf.switchRole(ROLE_Follwer)
		rf.currentTerm = args.Term
		rf.heartbeatFlag = 1
		// TODO 差异一 没有补 -1

	} else
	// candidate在相同任期收到，则转化为follow
	if rf.currentRole == ROLE_Candidate && rf.currentTerm == args.Term {
		rf.switchRole(ROLE_Follwer)
		rf.currentTerm = args.Term
		rf.heartbeatFlag = 1

		// TODO 差异一 没有补 -1
	} else if rf.currentRole == ROLE_Follwer {
		// follow
		rf.heartbeatFlag = 1
	}
	// leader不处理
	reply.Term = rf.currentTerm

}

// candidta发送给其他的follow去拉票
func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.votedCnt = 1
	rf.mu.Unlock()
	// 遍历每个节点
	for server, _ := range rf.peers {
		// 先跳过自己
		if server == rf.me {
			continue
		}
		// 接下来使用goroutine发送rpc
		go func(s int) {
			rf.mu.Lock()

			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: s,
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			ok := rf.sendRequestVote(s, &args, &reply)
			if !ok {
				fmt.Printf("[StartElection] id=%d request %d vote failed ...\n", rf.me, s)
			} else {
				fmt.Printf("[StartElection] %d send vote req succ to %d\n", rf.me, s)
			}
			rf.mu.Lock()
			// 处理回复任期更大的问题,直接降级为Follow
			if rf.currentTerm < reply.Term {
				rf.switchRole(ROLE_Follwer)
				rf.currentTerm = reply.Term
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				rf.votedCnt++
			}
			// 这里在缓存一下cnt的值
			cnt := rf.votedCnt
			role := rf.currentRole
			rf.mu.Unlock()

			// 票数过半，选举成功
			if cnt*2 >= len(rf.peers) {
				// 这里有可能处理 rpc 的时候，收到 rpc，变成了 follower，所以再校验一遍
				rf.mu.Lock()
				if rf.currentRole == ROLE_Candidate {
					rf.switchRole(ROLE_Leader)
					fmt.Printf("[StartElection] id=%d election succ, votecnt %d \n", rf.me, cnt)
					role = rf.currentRole
				}
				rf.mu.Unlock()
				if role == ROLE_Leader {
					rf.leaderHeartBeat() // 先主动 send heart beat 一次
				}
			}
		}(server)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 心跳
		select {
		// leader的心跳时间到了
		case <-rf.heartbeatTimer.C:
			if rf.currentRole == ROLE_Leader {
				rf.mu.Lock()
				// leader的心跳方法
				rf.leaderHeartBeat()
				// 重置定时器
				rf.heartbeatTimer.Reset(time.Millisecond * 100)
				rf.mu.Unlock()
			}
			// 选举时间到了
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			switch rf.currentRole {
			// follower开始投票
			case ROLE_Follwer:
				// follow转为 candidate参与选举
				rf.switchRole(ROLE_Candidate)
				rf.StartElection()
				// candidate参与选举
			case ROLE_Candidate:
				rf.StartElection()
			}
			rf.mu.Unlock()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.currentRole = ROLE_Follwer
	rf.heartbeatTimer = time.NewTimer(100 * time.Millisecond)
	rf.electionTimer = time.NewTimer(getRandomTimeout())

	rf.mu.Unlock()
	DPrintf("starting ... %d \n", me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
