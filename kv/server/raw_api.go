package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error:    err.Error(),
			Value:    nil,
			NotFound: true,
		}, err
	}
	val, err := reader.GetCF(req.GetCf(), req.Key)
	if val == nil {
		return &kvrpcpb.RawGetResponse{
			// Error:    err.Error(),
			Value:    nil,
			NotFound: true,
		}, nil
	}
	return &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: false,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	batch := []storage.Modify{storage.Modify{
		Data: storage.Put{
			Key:   req.GetKey(),
			Value: req.GetValue(),
			Cf:    req.GetCf(),
		},
	}}
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		return &kvrpcpb.RawPutResponse{
			Error: err.Error(),
		}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	batch := []storage.Modify{storage.Modify{
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf:  req.GetCf(),
		},
	}}
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{
			Error: err.Error(),
		}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawScanResponse{
			Error: err.Error(),
		}, nil
	}
	iter := reader.IterCF(req.GetCf())
	iter.Seek(req.GetStartKey())
	kvs := []*kvrpcpb.KvPair{}
	i := uint32(0)
	for ; i < req.GetLimit(); i++ {
		if !iter.Valid() {
			break
		}
		item := iter.Item()
		val, _ := item.Value()
		kvs = append(kvs, &kvrpcpb.KvPair{
			Error: nil,
			Key:   item.Key(),
			Value: val,
		})
		iter.Next()
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
