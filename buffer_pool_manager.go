package rosedb

import (
	"bytes"
	"container/list"
	"errors"
	"sync"

	"github.com/roseduan/rosedb/storage"
	// "github.com/stretchr/testify/assert"
)

type (
	buffer_pool_manager struct {
		pool_size   int8            //buffer pool size
		entry_array []storage.Entry //buffer
		replacer    *LRUReplacer
		free_list   *list.List //free buffer list
		latch       sync.Mutex
		entry_table map[string]frame_id_t //The mapping table,from entry id to frame id
		db          *RoseDB
	}
)

func New_buffer_pool_manager(size frame_id_t, db *RoseDB) *buffer_pool_manager {
	var i frame_id_t = 0
	List := list.New()
	for ; i < size; i++ {
		List.PushBack(i)
	}
	return &buffer_pool_manager{
		pool_size:   int8(size),
		entry_array: make([]storage.Entry, size),
		replacer:    New(int8(size)),
		free_list:   List,
		db:          db,
		entry_table: make(map[string]frame_id_t),
	}
}

func (manager *buffer_pool_manager) GetEntry(frame_id frame_id_t) *storage.Entry {
	return &(manager.entry_array[frame_id])
}

func (manager *buffer_pool_manager) GetAvailable() (fra frame_id_t, err error) {
	var frame_id frame_id_t = 0
	var entry *storage.Entry
	replacer := manager.replacer
	if manager.free_list.Len() != 0 {
		var next *list.Element
		frame_id = manager.free_list.Front().Value.(frame_id_t)
		front_element := manager.free_list.Front()
		next = front_element.Next()
		manager.free_list.Remove(front_element)
		front_element = next
		err := replacer.IntoReplacer(frame_id)
		if err != nil {
			return INVALID_FRAME_ID, err
		}
		return frame_id, nil
	}
	if replacer.Size() > 0 {
		replacer.Victim(&frame_id)
		replacer.IntoReplacer(frame_id)
		entry = manager.GetEntry(frame_id)
		err := manager.db.store(entry)
		manager.db.strIndex.mu.Lock()
		defer manager.db.strIndex.mu.Unlock()

		manager.db.setIndexer(entry)
		if err != nil {
			return INVALID_FRAME_ID, err
		}
		delete(manager.entry_table, entry.EntryId)
		return frame_id, nil
	}
	return INVALID_FRAME_ID, errors.New("there is no avaliable frame")
}

func (manager *buffer_pool_manager) GetFrame(entryid string) frame_id_t {
	for e, fra := range manager.entry_table {
		if e == entryid {
			return fra
		}
	}
	return INVALID_FRAME_ID
}

func (manager *buffer_pool_manager) WriteEntry(e *storage.Entry) error {
	manager.latch.Lock()
	defer manager.latch.Unlock()
	frame_id, err := manager.GetAvailable()
	if err != nil {
		return nil
	}
	manager.entry_array[frame_id] = *e
	manager.entry_table[e.EntryId] = frame_id
	return err
}

func (manager *buffer_pool_manager) FetchEntry(entryid string, key []byte) (*storage.Entry, error) {
	manager.latch.Lock()
	defer manager.latch.Unlock()
	var frame_id frame_id_t
	var entry *storage.Entry
	frame_id = manager.GetFrame(entryid)
	if frame_id != INVALID_FRAME_ID {
		entry = manager.GetEntry(frame_id)
		// manager.replacer.Pin(frame_id)
		err := manager.replacer.IntoReplacer(frame_id)
		if err != nil {
			return nil, err
		}
		return entry, nil
	}
	var err error
	frame_id, err = manager.GetAvailable()
	if err != nil || frame_id == INVALID_FRAME_ID {
		return nil, err
	}
	manager.entry_table[entryid] = frame_id
	entry, err = manager.db.getEntryVal(key)
	if err != nil {
		return nil, err
	}
	manager.entry_array[frame_id] = *entry
	manager.replacer.IntoReplacer(frame_id)
	return entry, nil
}

func (manager *buffer_pool_manager) FlushEntry(entryid string) error {
	manager.latch.Lock()
	defer manager.latch.Unlock()
	frame_id := manager.GetFrame(entryid)
	if frame_id == INVALID_FRAME_ID {
		return errors.New("in flush, frame is invalid")
	}
	entry := manager.GetEntry(frame_id)
	err := manager.db.store(entry)
	manager.db.setIndexer(entry)
	if err != nil {
		return err
	}
	return nil
}

func (manager *buffer_pool_manager) FlushAllEntry() error {
	for entryid, _ := range manager.entry_table {
		err := manager.FlushEntry(entryid)
		if err != nil {
			return err
		}
	}
	return nil
}

func (manager *buffer_pool_manager) IsInBuffer(key []byte) (bool, *storage.Entry) {
	entrys := []storage.Entry{}
	for _, e := range manager.entry_array {
		if e.Meta == nil {
			continue
		}
		if bytes.Equal(e.Meta.Key, key) {
			entrys = append(entrys, e)
		}
	}
	if len(entrys) == 0 {
		return false, nil
	}
	var max_ts uint64 = entrys[0].Timestamp
	var en storage.Entry = entrys[0]
	for _, e := range entrys {
		if e.Timestamp > max_ts {
			max_ts = e.Timestamp
			en = e
		}
	}
	return true, &en
}
