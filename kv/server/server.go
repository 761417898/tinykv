package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	startTS := int(req.Version)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	mxn := mvcc.NewMvccTxn(reader, uint64(startTS))
	lock, err := mxn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	var rsp kvrpcpb.GetResponse
	if lock != nil && lock.CheckLocked(req.Key, uint64(startTS)) {
		rsp.Error = new(kvrpcpb.KeyError)
		rsp.Error.Locked = lock.Info(req.Key)
		return &rsp, nil
	}
	value, err := mxn.GetValue(req.GetKey())
	if err != nil {
		return &rsp, err
	}
	if len(value) == 0 {
		rsp.NotFound = true
		return &rsp, nil
	}
	rsp.Value = value
	return &rsp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	var rsp kvrpcpb.PrewriteResponse
	if len(req.Mutations) == 0 {
		return &rsp, nil
	}
	startTS := int(req.GetStartVersion())
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	mxn := mvcc.NewMvccTxn(reader, uint64(startTS))
	// check lock
	locked := false
	conflict := false
	var allKeys [][]byte
	for _, mut := range req.Mutations {
		key := mut.GetKey()
		allKeys = append(allKeys, key)
		lock, err := mxn.GetLock(key)
		if err != nil {
			return nil, err
		}
		keyError := kvrpcpb.KeyError{
			Locked:    nil,
			Retryable: "",
			Abort:     "",
			Conflict:  nil,
		}
		if lock != nil && lock.CheckLocked(key, uint64(startTS)) {
			keyError.Locked = lock.Info(mut.Key)
			locked = true
		}

		write, commitTS, err := mxn.MostRecentWrite(mut.Key)
		if write != nil && mxn.StartTS >= write.StartTS && mxn.StartTS < commitTS {
			conflict = true
			keyError.Conflict = &kvrpcpb.WriteConflict{
				StartTs:    write.StartTS,
				ConflictTs: mxn.StartTS,
				Key:        mut.Key,
				Primary:    req.PrimaryLock,
			}
		}
		rsp.Errors = append(rsp.Errors, &keyError)
	}
	if locked || conflict {
		return &rsp, nil
	} else {
		rsp.Errors = nil
	}
	// write lock CF and default CF
	//server.Latches.WaitForLatches(allKeys)
	for _, mut := range req.Mutations {
		writeKind := mvcc.WriteKindFromProto(mut.Op)
		mxn.PutValue(mut.Key, mut.Value)
		mxn.PutLock(mut.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    writeKind,
		})
	}
	err = server.storage.Write(req.GetContext(), mxn.Writes())
	if err != nil {
		return nil, err
	}
	//server.Latches.ReleaseLatches(allKeys)
	return &rsp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	var rsp kvrpcpb.CommitResponse
	if len(req.Keys) == 0 {
		return &rsp, nil
	}
	startTS := int(req.GetStartVersion())
	commitTS := int(req.GetCommitVersion())
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	mxn := mvcc.NewMvccTxn(reader, uint64(startTS))
	// check lock
	for _, key := range req.Keys {
		lock, err := mxn.GetLock(key)
		if err != nil {
			return nil, err
		}
		keyError := kvrpcpb.KeyError{
			Locked:    nil,
			Retryable: "",
			Abort:     "",
			Conflict:  nil,
		}
		if lock == nil {
			tmpMxn := mvcc.MvccTxn{
				StartTS: uint64(commitTS),
				Reader:  reader,
			}
			val, err := tmpMxn.GetValue(key)
			if err == nil && len(val) != 0 {
				return &rsp, nil
			} else {
				val, err = mxn.Reader.GetCF(engine_util.CfDefault, key)
				//log.Infof("debug %d %s", len(val), err.Error())
				if err == nil && len(val) == 0 {
					rsp.Error = &keyError
					return &rsp, nil
				}
				return &rsp, nil
			}
		}
		log.Infof("debug: %s", lock.Info(key))
		if !lock.CheckLocked(key, uint64(startTS)) { // || (len(lock.Primary) != 0 && !lock.CheckLocked(key, mvcc.TsMax)) {
			keyError.Retryable = "true"
			rsp.Error = &keyError
			return &rsp, nil
		}
		if len(lock.Primary) != 0 && !lock.CheckLocked(key, mvcc.TsMax) {
			rsp.Error = &keyError
			return &rsp, nil
		}
		mxn.DeleteLock(key)
		mxn.PutWrite(key, uint64(commitTS), &mvcc.Write{
			StartTS: uint64(startTS),
			Kind:    lock.Kind,
		})

	}
	err = server.storage.Write(req.GetContext(), mxn.Writes())
	if err != nil {
		return nil, err
	}
	return &rsp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
