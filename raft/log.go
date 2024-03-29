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
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pkg/errors"
	"strconv"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	proposalEntries []*pb.Entry
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	rsp := &RaftLog{
		storage:         storage,
		committed:       0,
		applied:         0,
		stabled:         0,
		entries:         []pb.Entry{},
		pendingSnapshot: nil,
	}
	firstIdx, err := storage.FirstIndex()
	if err != nil {
		log.Fatal(err)
		return nil
	}
	lastIdx, err := storage.LastIndex()
	if err != nil {
		log.Fatal(err)
		return nil
	}
	initEntries, err := storage.Entries(firstIdx, lastIdx+1)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	rsp.entries = append(rsp.entries, initEntries...)
	rsp.stabled = lastIdx
	return rsp
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	rsp := []pb.Entry{}
	for _, entry := range l.entries {
		if entry.Data == nil {
			//continue
		}
		rsp = append(rsp, entry)
	}
	return rsp
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	if l.stabled == 0 {
		return l.entries
		// log.Infof("stabled = %d", l.stabled)
	}
	if !((l.stabled-l.entries[0].Index)+1 >= 0 && int((l.stabled-l.entries[0].Index)+1) < len(l.entries)) {
		return []pb.Entry{}
	}
	//xx, _ := l.storage.FirstIndex()
	//yy, _ := l.storage.LastIndex()
	//log.Infof("stabled=%d entry0.Index=%d  lastIdx=%d applied=%d commited=%d StorageFirstIndex=%d StorageLastIndex=%d", l.stabled, l.entries[0].Index, l.entries[len(l.entries)-1].Index, l.applied, l.committed, xx, yy)
	return l.entries[(l.stabled-l.entries[0].Index)+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if !(l.applied+1 >= l.entries[0].Index && l.applied+1 <= l.entries[len(l.entries)-1].Index) {
		return nil
	}
	if !(l.committed >= l.entries[0].Index && l.committed <= l.entries[len(l.entries)-1].Index) {
		return nil
	}
	appliedArrayIndex := l.applied + 1 - l.entries[0].Index
	committedArrayIndex := l.committed - l.entries[0].Index
	return l.entries[appliedArrayIndex : committedArrayIndex+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		if l.pendingSnapshot != nil {
			return l.pendingSnapshot.Metadata.Index
		}
		/*snap, err := l.storage.Snapshot()
		if err != ErrSnapshotTemporarilyUnavailable {
			return snap.Metadata.Index
		}*/
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 1, nil
	}
	if len(l.entries) == 0 {
		if l.pendingSnapshot != nil && i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		snapshot, err := l.storage.Snapshot()
		if err != ErrSnapshotTemporarilyUnavailable && i == snapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		return 1, errors.New("entry id empty")
	}
	if !(i >= l.entries[0].Index && i <= l.entries[len(l.entries)-1].Index) {
		return 1, errors.New("index" + strconv.Itoa(int(i)) + " out of range")
	}
	return l.entries[i-l.entries[0].Index].Term, nil
}

func (l *RaftLog) String() string {
	entriesStr := "{"
	for _, entry := range l.entries {
		entriesStr += " [" + entry.String() + "] "
	}
	entriesStr += "}"
	return fmt.Sprintf("RaftLog:{commit: %d applied: %d stabled: %d entries:%s}", l.committed, l.applied, l.stabled, entriesStr)
}

/*
	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry
*/
