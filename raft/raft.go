// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

const (
	DenyVoteIDOffset = 10000
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, err := c.Storage.InitialState()
	if c.peers == nil || len(c.peers) == 0 {
		c.peers = confState.Nodes
	}
	if err != nil {
		panic(err.Error())
	}
	rsp := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             []pb.Message{},
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
	for _, peer := range c.peers {
		rsp.votes[peer] = false
		rsp.votes[peer+DenyVoteIDOffset] = false
		rsp.Prs[peer] = &Progress{
			Match: 0,
			Next:  rsp.RaftLog.LastIndex() + 1,
		}
	}
	rsp.RaftLog.committed = hardState.Commit
	rsp.RaftLog.applied = c.Applied
	log.Infof("Raft init, peerid=%d, log commitid=%d, applied=%d, stabled=%d", rsp.id, rsp.RaftLog.committed, rsp.RaftLog.applied, rsp.RaftLog.stabled)
	return rsp
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.Prs[to].Next > r.RaftLog.LastIndex() ||
		!(r.Prs[to].Next >= r.RaftLog.entries[0].Index &&
			r.Prs[to].Next <= r.RaftLog.entries[len(r.RaftLog.entries)-1].Index) {
		return false
	}
	arrayIndex := r.Prs[to].Next - r.RaftLog.entries[0].Index
	var prevLogIndex, prevLogTerm uint64
	if arrayIndex == 0 {
		prevLogIndex = 0
		prevLogTerm = 0
	} else {
		prevLogIndex = uint64(int(r.RaftLog.entries[arrayIndex-1].Index))
		prevLogTerm, _ = r.RaftLog.Term(prevLogIndex)
	}
	appendEntries := []*pb.Entry{}
	for int(arrayIndex) < len(r.RaftLog.entries) {
		appendEntries = append(appendEntries, &r.RaftLog.entries[arrayIndex])
		arrayIndex++
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: appendEntries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  min(r.RaftLog.committed, r.Prs[to].Match),
	})
}

func (r *Raft) sendRequestVote() {
	voteReq := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      0,
		From:    r.id,
		Term:    r.Term,
	}
	// get last entry's term and index
	if len(r.RaftLog.entries) != 0 {
		voteReq.Index = r.RaftLog.LastIndex()
		voteReq.LogTerm, _ = r.RaftLog.Term(voteReq.Index)
	}
	for peer, _ := range r.Prs {
		if peer == r.id {
			continue
		}
		voteReq.To = peer
		r.msgs = append(r.msgs, voteReq)
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		if r.State == StateLeader {
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
			if err != nil {
				return
			}
		}
	}
	if r.electionElapsed == r.electionTimeout {
		if r.State == StateLeader {
			return
		}
		err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		if err != nil {
			return
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	log.Infof("peerid = %d timeout, becomes Candidate", r.id)
	r.State = StateCandidate
	for id, _ := range r.Prs {
		r.votes[id] = false
		r.votes[id+DenyVoteIDOffset] = false
	}
	r.Vote = r.id
	r.votes[r.id] = true
	r.Term++
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Infof("peerid = %d becomes leader", r.id)
	r.State = StateLeader
	r.Lead = r.id
	for _, peer := range r.Prs {
		peer.Match = 0
		peer.Next = r.RaftLog.LastIndex() + 1
	}

	noopEntry := pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
		Entries: []*pb.Entry{&pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Term:      r.Term,
			Index:     r.RaftLog.LastIndex() + 1,
			Data:      nil,
		}},
	}
	//r.msgs = append(r.msgs, noopEntry)
	err := r.Step(noopEntry)
	if err != nil {
		return
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleHup()
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			r.handleBeat(m)
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		if r.State == StateCandidate {
			r.handleRequestVoteResponse(m)
		}
	case pb.MessageType_MsgAppend:
		if len(m.Entries) == 0 && m.Index == 0 && m.LogTerm == 0 {
			r.handleHeartbeat(m)
		} else {
			r.handleAppendEntries(m)
		}
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgPropose:
		if r.State == StateLeader {
			r.handlePropose(m)
		}

	}
	/*for r.RaftLog.applied < r.RaftLog.committed {
		r.RaftLog.applied++
	}*/
	msgsStr := "{"
	for _, msg := range r.msgs {
		msgsStr += " [" + msg.String() + "] "
	}
	msgsStr += "}"

	//log.Infof("%s receives {%s}, sends {%s}", r.String(), m.String(), msgsStr)
	return nil
}

func (r *Raft) handleHup() {
	r.electionElapsed = -1 * (rand.Int() % r.electionTimeout)
	if r.State == StateLeader {
		return
	}
	r.becomeCandidate()
	r.sendRequestVote()
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

func (r *Raft) handleBeat(m pb.Message) {
	for follow, _ := range r.Prs {
		if follow == r.id {
			continue
		}
		r.sendHeartbeat(follow)
	}
	r.heartbeatElapsed = 0
	r.Vote = 0
}

func (r *Raft) handleRequestVote(m pb.Message) {
	// get last entry's term and index
	var lastEntryIndex, lastEntryTerm uint64
	lastEntryIndex = 0
	lastEntryTerm = 0
	if len(r.RaftLog.entries) != 0 {
		lastEntryIndex = r.RaftLog.LastIndex()
		lastEntryTerm, _ = r.RaftLog.Term(lastEntryIndex)
	}
	requestVoteResponse := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    m.To,
		Term:    m.Term,
		Reject:  true,
	}
	//log.Error(r.id, r.Vote, lastEntryIndex, m.Index, lastEntryTerm, m.LogTerm, (r.Vote == 0 || r.Vote == m.From) && (m.LogTerm > lastEntryTerm ||
	//	(m.LogTerm >= lastEntryTerm && m.Index >= lastEntryIndex)), r.Term, m.Term)
	if r.Term > m.Term {
		requestVoteResponse.Reject = true
		requestVoteResponse.Term = r.Term
	} else if r.Term == m.Term {
		if (r.Vote == 0 || r.Vote == m.From) && (m.LogTerm > lastEntryTerm ||
			(m.LogTerm >= lastEntryTerm && m.Index >= lastEntryIndex)) {
			requestVoteResponse.Reject = false
			requestVoteResponse.Term = r.Term
			r.Vote = m.From
			if m.From != r.id {
				r.State = StateFollower
				r.Lead = 0
				r.electionElapsed = 0
			}
		}
	} else {
		if m.LogTerm > lastEntryTerm ||
			(m.LogTerm == lastEntryTerm && m.Index >= lastEntryIndex) {
			requestVoteResponse.Reject = false
			r.Vote = m.From
		}
		r.Term = m.Term
		requestVoteResponse.Term = m.Term
		r.State = StateFollower
		r.Lead = 0
		r.electionElapsed = 0
	}
	if lastEntryTerm > m.LogTerm ||
		(lastEntryTerm == m.LogTerm && lastEntryIndex > m.Index) {
		requestVoteResponse.LogTerm = lastEntryTerm
		requestVoteResponse.Index = lastEntryIndex
	}
	r.msgs = append(r.msgs, requestVoteResponse)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	/*var lastEntryIndex, lastEntryTerm uint64
	lastEntryIndex = 0
	lastEntryTerm = 0
	if len(r.RaftLog.entries) != 0 {
		lastEntryIndex = r.RaftLog.LastIndex()f
		lastEntryTerm, _ = r.RaftLog.Term(lastEntryIndex)
	}*/
	log.Infof("peerid = %d, term = %d receives %s", r.id, r.Term, m.String())
	if r.Term < m.Term {
		r.State = StateFollower
		r.Lead = 0
		return
	}
	if m.Reject {
		r.votes[m.From] = false
		r.votes[m.From+DenyVoteIDOffset] = true
	} else {
		r.votes[m.From] = true
	}
	voteCnt := 0
	denyCnt := 0
	totalCnt := len(r.Prs)
	for id, _ := range r.Prs {
		if r.votes[id] {
			voteCnt++
		} else if r.votes[id+DenyVoteIDOffset] {
			denyCnt++
		}
	}
	if voteCnt*2 > totalCnt && r.State != StateLeader {
		r.becomeLeader()
	}
	if denyCnt*2 > totalCnt {
		r.State = StateFollower
		r.Vote = 0
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if r.Term <= m.Term {
		if r.id != m.From {
			r.becomeFollower(m.Term, m.From)
			r.electionElapsed = 0
			if r.Vote != m.From {
				r.Vote = 0
			}
		}
	} else {
		return
	}
	rsp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    m.To,
		Term:    r.Term,
		Reject:  true,
	}
	offset := 0
	if m.Index != 0 || m.LogTerm != 0 {
		localPrevLogTerm, err := r.RaftLog.Term(m.Index)
		if err != nil || (m.LogTerm != localPrevLogTerm) {
			r.msgs = append(r.msgs, rsp)
			return
		}
		prevLogArrayIndex := int(m.Index - r.RaftLog.entries[0].Index)
		offset = prevLogArrayIndex + 1
	}
	idx := 0
	oldEntries := make([]pb.Entry, len(r.RaftLog.entries))
	copy(oldEntries, r.RaftLog.entries)
	isConfilict := false
	for offset+idx < len(r.RaftLog.entries) && idx < len(m.Entries) {
		if r.RaftLog.entries[offset+idx].Index != (*m.Entries[idx]).Index ||
			r.RaftLog.entries[offset+idx].Term != (*m.Entries[idx]).Term ||
			string(r.RaftLog.entries[offset+idx].Data) != string((*m.Entries[idx]).Data) {
			isConfilict = true
		}
		r.RaftLog.entries[offset+idx] = *m.Entries[idx]
		idx++
	}
	for idx < len(m.Entries) {
		r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[idx])
		idx++
	}
	for offset+idx < len(r.RaftLog.entries) {
		if isConfilict {
			r.RaftLog.entries = r.RaftLog.entries[:offset+idx]
			break
		}
		if offset+idx >= 1 && (r.RaftLog.entries[offset+idx].Term < r.RaftLog.entries[offset+idx-1].Term ||
			r.RaftLog.entries[offset+idx].Index <= r.RaftLog.entries[offset+idx-1].Index) {
			r.RaftLog.entries = r.RaftLog.entries[:offset+idx]
			break
		}
		idx++
	}
	idx = 0
	for idx < len(oldEntries) && idx < len(r.RaftLog.entries) {
		if oldEntries[idx].Index != r.RaftLog.entries[idx].Index ||
			oldEntries[idx].Term != r.RaftLog.entries[idx].Term {
			r.RaftLog.stabled = min(r.RaftLog.stabled, oldEntries[idx].Index-1)
			break
		}
		idx++
	}
	rsp.Reject = false
	rsp.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, rsp)
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
		r.RaftLog.committed = min(r.RaftLog.committed, m.Index+uint64(len(m.Entries)))
	}
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if r.Term < m.Term {
		// becomeFollow
		r.State = StateFollower
		r.Vote = 0
		return
	}
	if m.Reject {
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
		return
	} else {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
	}
	N := r.RaftLog.LastIndex()
	// log.Error(N, r.RaftLog.entries)
	for N >= r.RaftLog.committed+1 {
		numMatched := 0
		for _, progres := range r.Prs {
			if progres.Match >= N {
				numMatched++
			}
		}
		term, err := r.RaftLog.Term(N)
		if err != nil {
			return
		}
		if numMatched*2 > len(r.Prs) && r.Term == term {
			if r.RaftLog.committed != N {
				r.RaftLog.committed = N
				for peer, _ := range r.Prs {
					if peer == r.id {
						continue
					}
					r.msgs = append(r.msgs, pb.Message{
						MsgType: pb.MessageType_MsgAppend,
						To:      peer,
						From:    r.id,
						Term:    r.Term,
						Commit:  r.RaftLog.committed,
					})
				}
			}
			break
		}
		N--
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	rsp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    m.To,
		Term:    m.Term,
	}
	if m.Term < r.Term {
		rsp.Term = r.Term
		r.msgs = append(r.msgs, rsp)
		return
	}
	r.becomeFollower(m.Term, m.From)
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.Vote = 0
	if m.Commit >= r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.msgs = append(r.msgs, rsp)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// Your Code Here (2A).
	if r.Term < m.Term {
		// becomeFollow
		r.becomeFollower(m.Term, m.From)
		r.electionElapsed = 0
		r.Vote = 0
		return
	}
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	if lastIndex == 0 {
		lastIndex = r.RaftLog.committed
		for peer, _ := range r.Prs {
			if peer != r.id {
				r.Prs[peer].Match = r.RaftLog.committed
				r.Prs[peer].Next = r.RaftLog.committed + 1
			}
		}
	}
	for i, entry := range m.Entries {
		entry.Index = lastIndex + uint64(i) + 1
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	//r.Prs[r.id].Match = lastIndex
	//r.Prs[r.id].Next = lastIndex + 1
	log.Infof("Handle Proposal %s, %s", r.String(), m.String())
	for peer, _ := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
}

/*
type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64
*/

func (r *Raft) String() string {
	return fmt.Sprintf("RaftState:{peerid: %d Term: %d Vote: %d State: %s}",
		r.id, r.Term, r.Vote, r.State.String())
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
