package file

import (
	"errors"
	"path/filepath"

	"github.com/radek-ryckowski/monofs/kvstore"
	"github.com/radek-ryckowski/monofs/utils"
)

// FsFileEngine managing pool of files assign proper client to file handle and managing space on disk
// it also managing locking and unlocking files
type FsFileEngine struct {
	hash  string
	path  string
	inode uint64
	kvs   *kvstore.KVStore
}

// NewFsFileEngine creates new FsFileEngine object
func NewFsFileEngine(inode uint64, path string, hash string) (*FsFileEngine, error) {
	kvs, err := kvstore.NewKVStore(filepath.Join(path, hash), 4096*2)
	if err != nil {
		return nil, err
	}
	return &FsFileEngine{
		inode: inode,
		path:  path,
		hash:  hash,
		kvs:   kvs,
	}, nil
}

// TODO use custo, KV db for every file and on close index should keep trace of current blocks in 4k pages ex: 0 -> 2.4 - block 0 == file 2 block 4 , sync asynchornous send data fro db compressed with snappy to proxy / S3 etc
// reading object from proxy split it to 4k blocks put them to custom db ( index should nbe fast as it is 1:1 mapping)

// PickBlock calculate block based on offset
func PickBlocks(offset uint64, dataLen uint64) ([]uint64, error) {
	if dataLen < offset {
		return nil, errors.New("offset is bigger than data length")
	}
	blocks := make([]uint64, 2)
	blocks[0] = offset / 4096
	blocks[1] = (offset + dataLen) / 4096
	return blocks, nil
}

//AllocateBlocks allocate blocks for a offset and data length

func (fs *FsFileEngine) AllocateBlocks(offset uint64, dataLen uint64) (map[uint64][]byte, error) {
	blocks, err := PickBlocks(offset, dataLen)
	if err != nil {
		return nil, err
	}
	blocksData := make(map[uint64][]byte)
	for _, block := range blocks {
		blocksData[block] = make([]byte, 4096)
		// read block from kv store
		blockData, err := fs.kvs.Get(utils.Uint64ToBytes(block))
		if err != nil {
			if err != kvstore.ErrItemNotFound {
				return nil, err
			}
			continue
		}
		copy(blocksData[block], blockData)
	}
	return blocksData, nil
}

// WriteBlock write block to kv store
func (fs *FsFileEngine) WriteBlock(block uint64, data []byte) error {
	return fs.kvs.Put(utils.Uint64ToBytes(block), data)
}

// ReadBlock read block from kv store
func (fs *FsFileEngine) ReadBlock(block uint64) ([]byte, error) {
	return fs.kvs.Get(utils.Uint64ToBytes(block))
}
