package kvstore

import (
	"fmt"
	"io"
	"os"

	"hash/crc32"

	"github.com/radek-ryckowski/monofs/utils"
)

const (
	Tombstoned = iota + 1
	metaSize   = 13
	headerSize = 9
)

type Record struct {
	Flags int8
	Key   []byte
	Value []byte
}

func SetBit(n int8, pos uint) int8 {
	n |= (1 << pos)
	return n
}

func ClearBit(n int8, pos uint) int8 {
	mask := int8(^(1 << pos))
	n &= mask
	return n
}

func HasBit(n int8, pos uint) bool {
	val := n & (1 << pos)
	return (val > 0)
}

func NewRecord(key []byte, value []byte, flags int8) *Record {
	return &Record{Key: key, Value: value}
}

func (r *Record) IsTombstoned() bool {
	return HasBit(r.Flags, Tombstoned)
}

func (r *Record) Tombstone() {
	r.Flags = SetBit(r.Flags, Tombstoned)
}

func (r *Record) Untombstone() {
	r.Flags = ClearBit(r.Flags, Tombstoned)
}

func (r *Record) CalculateCRC(b []byte) uint32 {
	return crc32.ChecksumIEEE(b)
}

func (r *Record) Encode() []byte {
	buf := make([]byte, metaSize+len(r.Key)+len(r.Value)) //1 + 4 + 4 +len(key) + len(value) + 4
	buf[0] = byte(r.Flags)
	valueLen := uint32(len(r.Value))
	keyLen := uint32(len(r.Key))
	copy(buf[1:5], utils.Uint32ToBytes(keyLen))
	copy(buf[5:9], utils.Uint32ToBytes(valueLen))
	keyEndPos := headerSize + keyLen
	valEndPos := keyEndPos + valueLen
	copy(buf[headerSize:keyEndPos], r.Key)
	copy(buf[keyEndPos:valEndPos], r.Value)
	crc := r.CalculateCRC(buf[0:valEndPos])
	copy(buf[valEndPos:], utils.Uint32ToBytes(crc))
	return buf
}

func (r *Record) Decode(data []byte) error {
	r.Flags = int8(data[0])
	keyLen := utils.BytesToUint32(data[1:5])
	valueLen := utils.BytesToUint32(data[5:9])
	keyEndPos := headerSize + keyLen
	valEndPos := keyEndPos + valueLen
	r.Key = make([]byte, keyLen)
	copy(r.Key, data[headerSize:keyEndPos])
	r.Value = make([]byte, valueLen)
	copy(r.Value, data[keyEndPos:valEndPos])
	crc32 := utils.BytesToUint32(data[valEndPos : valEndPos+4])
	crc := r.CalculateCRC(data[0:valEndPos])
	if crc != crc32 {
		return fmt.Errorf("CRC check failed %d != %d", crc, crc32)
	}
	return nil
}

func (r *Record) Write(w io.Writer) (int, error) {
	return w.Write(r.Encode())
}

func (r *Record) ReadAt(reader io.ReaderAt, data []byte, offset int64) (int, error) {
	n, err := reader.ReadAt(data, offset)
	if err != nil {
		return 0, err
	}
	if n != len(data) {
		return 0, fmt.Errorf("failed to read %d bytes, only read %d", len(data), n)
	}
	return n, r.Decode(data)
}

func (r *Record) RawSize() int {
	return metaSize + len(r.Key) + len(r.Value)
}

func (r *Record) Read(f *os.File, maxRecordSize int) (int, error) {
	buf := make([]byte, maxRecordSize+metaSize)
	n, err := f.Read(buf)
	if err != nil {
		return 0, err
	}
	return n, r.Decode(buf)
}
