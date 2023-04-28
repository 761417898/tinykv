package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	configure *config.Config
	dbEngine  *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		configure: conf,
		dbEngine:  nil,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	db := engine_util.CreateDB(s.configure.DBPath, false)
	s.dbEngine = engine_util.NewEngines(db, nil, s.configure.DBPath, "")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.dbEngine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := standAloneStorageReader{
		txn: s.dbEngine.Kv.NewTransaction(false),
	}
	return &reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writeBatch := new(engine_util.WriteBatch)
	for _, row := range batch {
		switch row.Data.(type) {
		case storage.Put:
			writeBatch.SetCF(row.Cf(), row.Key(), row.Value())
		case storage.Delete:
			writeBatch.DeleteCF(row.Cf(), row.Key())
		}
	}
	err := s.dbEngine.WriteKV(writeBatch)
	if err != nil {
		return err
	}
	return nil
}

type standAloneStorageReader struct {
	standAloneStorage *StandAloneStorage
	txn               *badger.Txn
}

func (reader *standAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err != nil && err.Error() == "Key not found" {
		return nil, nil
	}
	return val, err
}

func (reader *standAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}

func (reader *standAloneStorageReader) Close() {
	reader.txn.Discard()
}
