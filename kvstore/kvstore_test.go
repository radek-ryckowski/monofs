package kvstore

import (
	"errors"
	"fmt"
	"testing"
)

func TestKVStoreIndex(t *testing.T) {
	filepath := t.TempDir() + "/kvstore.db"
	kv, err := NewKVStore(filepath, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		key := []byte("key" + fmt.Sprintf("%d", i))
		value := []byte("value" + fmt.Sprintf("%d", i))
		err = kv.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}
	kv.Close()
	kvNew, err := NewKVStore(filepath, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer kvNew.Close()
	err = kvNew.RebuildIndex()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		key := []byte("key" + fmt.Sprintf("%d", i))
		valueExp := []byte("value" + fmt.Sprintf("%d", i))
		value, err := kvNew.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if string(valueExp) != string(value) {
			t.Fatalf("expected value to be %s, got %s", valueExp, value)
		}
	}
}

func TestKVStore(t *testing.T) {
	filePath := t.TempDir() + "/kvstore.db"
	kv, err := NewKVStore(filePath, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	err = kv.Put([]byte("hello"), []byte("world"))
	if err != nil {
		t.Fatal(err)
	}

	err = kv.Put([]byte("allo"), []byte("dlrow"))
	if err != nil {
		t.Fatal(err)
	}
	value, err := kv.Get([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "world" {
		t.Fatalf("expected value to be world, got %s", value)
	}
	value, err = kv.Get([]byte("allo"))
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "dlrow" {
		t.Fatalf("expected value to be dlrow, got %s", value)
	}

	kv.Delete([]byte("allo"))
	_, err = kv.Get([]byte("allo"))
	if !errors.Is(err, ErrItemNotFound) {
		t.Fatalf("expected error to be ErrItemNotFound, got %s", err)
	}
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func TestKVStoreSearch(t *testing.T) {
	tests := []struct {
		name      string
		expected  []string
		stored    []string
		searchKey []byte
		revert    bool
	}{
		{
			name:      "search forward",
			stored:    []string{"hello", "hello1", "hello2", "hello3", "hello4", "hello5"},
			expected:  []string{"hello2", "hello3", "hello4", "hello5"},
			searchKey: []byte("hello2"),
			revert:    false,
		},
		{
			name:      "search backward",
			stored:    []string{"hello", "hello1", "hello2", "hello3", "hello4", "hello5"},
			expected:  []string{"hello", "hello1", "hello2", "hello3"},
			searchKey: []byte("hello3"),
			revert:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filePath := t.TempDir() + "/kvstore.db"
			kv, err := NewKVStore(filePath, 1024*1024)
			if err != nil {
				t.Fatal(err)
			}
			defer kv.Close()
			for _, item := range test.stored {
				err = kv.Put([]byte(item), []byte("world"))
				if err != nil {
					t.Fatal(err)
				}
			}
			ret, err := kv.Search(test.searchKey, test.revert)
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range ret {
				if ok := contains(test.expected, string(item.Key)); !ok {
					t.Fatalf("expected key to be in %v, got %s", test.expected, string(item.Key))
				}
			}
			if len(ret) != len(test.expected) {
				t.Fatalf("expected len to be %d, got %d", len(test.expected), len(ret))
			}
		})
	}
}
