package kvstore

import (
	"fmt"
	"os"
	"sync"

	"github.com/radek-ryckowski/monofs/utils"
)

type Store interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	Flush() error
	Search(key []byte, revert bool) ([]*Record, error)
	Send(storeVersion string) error
	Retrieve(storeVersion string) error
	RebuildIndex() error
	Close() error
}

type KVStore struct {
	Store
	wmutex        *sync.RWMutex
	index         *Index
	path          string
	file          *os.File
	maxRecordSize int
}

func NewKVStore(storagePath string, maxRecordSize int) (*KVStore, error) {
	file, err := os.OpenFile(storagePath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %s for write, %w", storagePath, err)
	}
	return &KVStore{
		file:          file,
		path:          storagePath,
		maxRecordSize: maxRecordSize,
		index:         NewIndex(),
		wmutex:        &sync.RWMutex{},
	}, nil
}

func (kv *KVStore) Get(key []byte) ([]byte, error) {
	offsetRaw, err := kv.index.Get(key)
	if err != nil {
		return nil, err
	}
	offset := int64(utils.BytesToUint32(offsetRaw))
	record, err := kv.recordAt(offset)
	if err != nil {
		return nil, err
	}
	return record.Value, nil
}

func (kv *KVStore) set(key []byte, value []byte, deleted bool) error {
	kv.wmutex.Lock()
	defer kv.wmutex.Unlock()
	offset, err := kv.file.Seek(0, 2)
	if err != nil {
		return err
	}
	flags := int8(0)
	record := NewRecord(key, value, flags)
	if deleted {
		record.Tombstone()
	}
	_, err = record.Write(kv.file)
	if err != nil {
		return err
	}
	if record.IsTombstoned() {
		kv.index.Delete(key)
		return nil
	}
	return kv.index.Add(key, utils.Uint32ToBytes(uint32(offset)))
}

func (kv *KVStore) Delete(key []byte) error {
	return kv.set(key, []byte{0}, true)
}

func (kv *KVStore) Put(key []byte, value []byte) error {
	return kv.set(key, value, false)
}

func (kv *KVStore) RebuildIndex() error {
	offset := uint32(0)
	file, err := os.OpenFile(kv.path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("could not open file: %s , %w", kv.path, err)
	}
	scaner, err := NewScanner(file, kv.maxRecordSize)
	if err != nil {
		return err
	}
	kv.wmutex.Lock()
	defer kv.wmutex.Unlock()
	for scaner.Scan() {
		record, err := scaner.Record()
		if err != nil {
			return err
		}
		if err := kv.index.Add(record.Key, utils.Uint32ToBytes(offset)); err != nil {
			return err
		}
		offset += uint32(record.RawSize())
	}
	return nil
}

func (kv *KVStore) Close() error {
	return kv.file.Close()
}

func (kv *KVStore) recordAt(offset int64) (*Record, error) {
	kv.wmutex.RLock()
	defer kv.wmutex.RUnlock()
	_, err := kv.file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	record := &Record{}
	_, err = record.Read(kv.file, kv.maxRecordSize)
	return record, err
}

func (kv *KVStore) Flush() error {
	return kv.file.Sync()
}

func (kv *KVStore) Search(key []byte, revert bool) ([]*Record, error) {
	items := kv.index.Search(key, revert)
	records := make([]*Record, len(items))
	for i, item := range items {
		offset := int64(utils.BytesToUint32(item.Val))
		record, err := kv.recordAt(offset)
		if err != nil {
			return nil, err
		}
		records[i] = record
	}
	return records, nil
}
