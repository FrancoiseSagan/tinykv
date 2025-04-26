package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	dbPath string
	db     *badger.DB
}

type StandAloneReader struct {
	db  *badger.DB
	txn *badger.Txn
}

func (sr *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, _ := engine_util.GetCF(sr.db, cf, key)
	return value, nil
}

func (sr *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr *StandAloneReader) Close() {
	sr.txn.Discard()
	return
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	var sastorage StandAloneStorage
	sastorage.dbPath = conf.DBPath
	return &sastorage
}

func (s *StandAloneStorage) Start() error {
	s.db = engine_util.CreateDB(s.dbPath, false)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	err := s.db.Close()
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneReader{db: s.db, txn: s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	var wb engine_util.WriteBatch
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			{
				wb.SetCF(modify.Data.(storage.Put).Cf, modify.Data.(storage.Put).Key, modify.Data.(storage.Put).Value)
			}
		case storage.Delete:
			{
				wb.DeleteCF(modify.Data.(storage.Delete).Cf, modify.Data.(storage.Delete).Key)
			}
		}
	}

	err := wb.WriteToDB(s.db)
	return err
}
