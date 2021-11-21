package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

const RaftPath = "/tmp/raft"

type storageReaderTxn struct {
	txn *badger.Txn
}

func NewstorageReaderTxn(txn *badger.Txn) *storageReaderTxn {
	return &storageReaderTxn{
		txn: txn,
	}
}

// Get value based on key from CF
func (s *storageReaderTxn) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

// Gets CF iterator
// Rewind would rewind the iterator cursor all the way to zero-th position
func (s *storageReaderTxn) IterCF(cf string) engine_util.DBIterator {
	it := engine_util.NewCFIterator(cf, s.txn)
	it.Rewind()
	return it
}

func (s *storageReaderTxn) Close() {
	s.txn.Discard()
}

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	Engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	storage := StandAloneStorage{}
	kv := engine_util.CreateDB(conf.DBPath, false)
	raft := engine_util.CreateDB(RaftPath, true)
	storage.Engine = engine_util.NewEngines(kv, raft, conf.DBPath, RaftPath)
	return &storage

}

// start standAloneStorage
func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	log.Info("=====start standAloneStorage=====")
	return nil
}

// stop standAloneStorage
func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	log.Info("=====stop standAloneStorage=====")
	return s.Engine.Close()

}

// Reader
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.Engine.Kv.NewTransaction(false)
	storageReader := NewstorageReaderTxn(txn)
	return storageReader, nil
}

// Write
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, entry := range batch {
		wb := engine_util.WriteBatch{}
		switch data := entry.Data.(type) {
		case storage.Put:
			wb.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			wb.DeleteCF(data.Cf, data.Key)
		}
		err := s.Engine.WriteKV(&wb)
		if err != nil {
			return err
		}
	}
	return nil
}
