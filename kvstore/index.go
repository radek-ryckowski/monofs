package kvstore

import (
	"bytes"
	"errors"

	"github.com/tidwall/btree"
)

var (
	ErrItemNotFound = errors.New("not found")
)

type Index struct {
	// contains filtered or unexported fields
	t *btree.BTreeG[Item]
}

type Item struct {
	Key, Val []byte
}

func byKeys(a, b Item) bool {
	return bytes.Compare(a.Key, b.Key) < 0
}

func NewIndex() *Index {
	return &Index{
		t: btree.NewBTreeG[Item](byKeys),
	}
}

func (i *Index) Add(key []byte, value []byte) error {
	item := Item{
		Key: key,
		Val: value,
	}
	i.t.Set(item)
	return nil
}

func (i *Index) Get(key []byte) ([]byte, error) {
	item := Item{
		Key: key,
	}
	x, ok := i.t.Get(item)
	if !ok {
		return nil, ErrItemNotFound
	}
	return x.Val, nil
}

func (i *Index) Delete(key []byte) {
	item := Item{
		Key: key,
	}
	i.t.Delete(item)
}

func (i *Index) Search(key []byte, descend bool) []*Item {
	item := Item{
		Key: key,
	}
	var items []*Item
	if descend {
		i.t.Descend(item, func(item Item) bool {
			items = append(items, &item)
			return true
		})
	} else {
		i.t.Ascend(item, func(item Item) bool {
			items = append(items, &item)
			return true
		})
	}
	return items
}
