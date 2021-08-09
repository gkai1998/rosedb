package rosedb

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/roseduan/rosedb/storage"
)

func Test_sample1(t *testing.T) {
	db := InitDb()
	defer db.Close()
	manager := New_buffer_pool_manager(3, db)
	e1 := storage.NewEntryNoExtra([]byte("hello"), []byte("d"), String, StringSet)
	e2 := storage.NewEntryNoExtra([]byte("hello"), []byte("dd"), String, StringSet)
	e3 := storage.NewEntryNoExtra([]byte("hello"), []byte("ddd"), String, StringSet)
	e4 := storage.NewEntryNoExtra([]byte("hello"), []byte("dddd"), String, StringSet)

	frame_id, err := manager.GetAvailable()
	if frame_id != 0 {
		t.Errorf("frame id is %d,want %d", frame_id, 0)
	}
	if manager.replacer.Size() != 1 {
		t.Errorf("replace size is %d,want %d", manager.replacer.Size(), 1)
	}
	if manager.free_list.Len() != 2 {
		t.Errorf("free list size is %d,want %d", manager.free_list.Len(), 2)
	}
	manager.entry_array[frame_id] = *e1
	manager.entry_table[e1.EntryId] = frame_id

	frame_id, err = manager.GetAvailable()
	if err != nil {
		t.Errorf("error")
	}
	if frame_id != 1 {
		t.Errorf("frame id is %d,want %d", frame_id, 1)
	}
	if manager.replacer.Size() != 2 {
		t.Errorf("replace size is %d,want %d", manager.replacer.Size(), 2)
	}
	if manager.free_list.Len() != 1 {
		t.Errorf("free list size is %d,want %d", manager.free_list.Len(), 1)
	}
	manager.entry_array[frame_id] = *e2
	manager.entry_table[e2.EntryId] = frame_id

	frame_id, err = manager.GetAvailable()
	if err != nil {
		t.Errorf("error")
	}
	if frame_id != 2 {
		t.Errorf("frame id is %d,want %d", frame_id, 2)
	}
	if manager.replacer.Size() != 3 {
		t.Errorf("replace size is %d,want %d", manager.replacer.Size(), 3)
	}
	if manager.free_list.Len() != 0 {
		t.Errorf("free list size is %d,want %d", manager.free_list.Len(), 0)
	}
	manager.entry_array[frame_id] = *e3
	manager.entry_table[e3.EntryId] = frame_id

	frame_id, err = manager.GetAvailable()
	if err != nil {
		t.Errorf("error")
	}
	if frame_id != 0 {
		t.Errorf("frame id is %d,want %d", frame_id, 0)
	}
	if manager.replacer.Size() != 3 {
		t.Errorf("replace size is %d,want %d", manager.replacer.Size(), 3)
	}
	if manager.free_list.Len() != 0 {
		t.Errorf("free list size is %d,want %d", manager.free_list.Len(), 0)
	}
	manager.entry_array[frame_id] = *e4
	manager.entry_table[e4.EntryId] = frame_id
}

func Test_sample2(t *testing.T) {
	db := InitDb()
	defer db.Close()
	manager := New_buffer_pool_manager(3, db)
	e1 := storage.NewEntryNoExtra([]byte("hello1"), []byte("d"), String, StringSet)
	e2 := storage.NewEntryNoExtra([]byte("hello2"), []byte("dd"), String, StringSet)
	e3 := storage.NewEntryNoExtra([]byte("hello3"), []byte("ddd"), String, StringSet)
	e4 := storage.NewEntryNoExtra([]byte("hello4"), []byte("dddd"), String, StringSet)
	e5 := storage.NewEntryNoExtra([]byte("hello5"), []byte("ddddd"), String, StringSet)
	e6 := storage.NewEntryNoExtra([]byte("hello6"), []byte("dddddd"), String, StringSet)
	e7 := storage.NewEntryNoExtra([]byte("hello7"), []byte("ddddddd"), String, StringSet)
	manager.WriteEntry(e1)
	manager.WriteEntry(e2)
	manager.WriteEntry(e3)
	manager.WriteEntry(e4)
	manager.WriteEntry(e5)
	manager.WriteEntry(e6)
	manager.WriteEntry(e7)
	re1, err := manager.FetchEntry(e1.EntryId, []byte("hello1"))
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(re1.Meta.Key, e1.Meta.Key) {
		t.Errorf("re1 key not match")
	}
	if !bytes.Equal(re1.Meta.Value, e1.Meta.Value) {
		t.Errorf("re1 value not match")
	}
	if manager.entry_table[e1.EntryId] != 1 {
		t.Errorf("re1 entry table postion is wrong")
	}
	if reflect.DeepEqual(manager.entry_array[1], *e1) {
		t.Errorf("re1 entry not equal to real entry")
	}

	re2, err := manager.FetchEntry(e2.EntryId, []byte("hello2"))
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(re2.Meta.Key, e2.Meta.Key) {
		t.Errorf("re2 key not match")
	}
	if !bytes.Equal(re2.Meta.Value, e2.Meta.Value) {
		t.Errorf("re2 value not match")
	}
	if manager.entry_table[e2.EntryId] != 2 {
		t.Errorf("re2 entry table postion is wrong")
	}
	if reflect.DeepEqual(manager.entry_array[2], *e2) {
		t.Errorf("re2 entry not equal to real entry")
	}

	re3, err := manager.FetchEntry(e3.EntryId, []byte("hello3"))
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(re3.Meta.Key, e3.Meta.Key) {
		t.Errorf("re3 key not match")
	}
	if !bytes.Equal(re3.Meta.Value, e3.Meta.Value) {
		t.Errorf("re3 value not match")
	}
	if manager.entry_table[e3.EntryId] != 0 {
		t.Errorf("re3 entry table postion is wrong")
	}
	if reflect.DeepEqual(manager.entry_array[0], *e3) {
		t.Errorf("re3 entry not equal to real entry")
	}

	re4, err := manager.FetchEntry(e4.EntryId, []byte("hello4"))
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(re4.Meta.Key, e4.Meta.Key) {
		t.Errorf("re4 key not match")
	}
	if !bytes.Equal(re4.Meta.Value, e4.Meta.Value) {
		t.Errorf("re4 value not match")
	}
	if manager.entry_table[e4.EntryId] != 1 {
		t.Errorf("re4 entry table postion is wrong")
	}
	if reflect.DeepEqual(manager.entry_array[1], *e4) {
		t.Errorf("re4 entry not equal to real entry")
	}

	re5, err := manager.FetchEntry(e5.EntryId, []byte("hello5"))
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(re5.Meta.Key, e5.Meta.Key) {
		t.Errorf("re5 key not match")
	}
	if !bytes.Equal(re5.Meta.Value, e5.Meta.Value) {
		t.Errorf("re5 value not match")
	}
	if manager.entry_table[e5.EntryId] != 2 {
		t.Errorf("re5 entry table postion is wrong")
	}
	if reflect.DeepEqual(manager.entry_array[2], *e5) {
		t.Errorf("re5 entry not equal to real entry")
	}

	re6, err := manager.FetchEntry(e6.EntryId, []byte("hello6"))
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(re6.Meta.Key, e6.Meta.Key) {
		t.Errorf("re6 key not match")
	}
	if !bytes.Equal(re6.Meta.Value, e6.Meta.Value) {
		t.Errorf("re6 value not match")
	}
	if manager.entry_table[e6.EntryId] != 0 {
		t.Errorf("re6 entry table postion is wrong")
	}
	if reflect.DeepEqual(manager.entry_array[0], *e6) {
		t.Errorf("re6 entry not equal to real entry")
	}
	re7, err := manager.FetchEntry(e7.EntryId, []byte("hello7"))
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(re7.Meta.Key, e7.Meta.Key) {
		t.Errorf("re7 key not match")
	}
	if !bytes.Equal(re7.Meta.Value, e7.Meta.Value) {
		t.Errorf("re7 value not match")
	}
	if manager.entry_table[e7.EntryId] != 1 {
		t.Errorf("re7 entry table postion is wrong")
	}
	if reflect.DeepEqual(manager.entry_array[1], *e7) {
		t.Errorf("re7 entry not equal to real entry")
	}
	err = db.manager.FlushAllEntry()
	if err != nil {
		t.Errorf("cannot flush")
	}
}
