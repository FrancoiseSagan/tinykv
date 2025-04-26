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
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())

	response := kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: value == nil,
	}

	return &response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {

	batch := make([]storage.Modify, 0)
	batch = append(batch, storage.Modify{Data: storage.Put{
		Cf:    req.Cf,
		Key:   req.Key,
		Value: req.Value,
	}})

	err := server.storage.Write(nil, batch)
	return nil, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {

	batch := make([]storage.Modify, 0)
	batch = append(batch, storage.Modify{Data: storage.Delete{
		Cf:  req.Cf,
		Key: req.Key,
	}})

	err := server.storage.Write(nil, batch)
	return nil, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {

	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	kvs := make([]*kvrpcpb.KvPair, 0)
	for iter.Valid() {
		key := iter.Item().Key()
		value, err := iter.Item().Value()
		if err != nil {
			return nil, err
		}
		kv := kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		}
		kvs = append(kvs, &kv)
		if uint32(len(kvs)) >= req.Limit {
			break
		}
		iter.Next()
	}

	iter.Close()
	reader.Close()

	response := kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}
	return &response, nil
}
