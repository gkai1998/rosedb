package rosedb

import (
	"errors"
	"sync"
)

const (
	INVALID_FRAME_ID frame_id_t = -1
)

type (
	frame_id_t int8

	LRUReplacer struct {
		size           int8  //buffer size
		timestamp      int64 //for lru algorithm
		replacer_mutex sync.Mutex
		frame_map      map[frame_id_t]int64 //frame_id->timestamp
	}
)

func New(size int8) *LRUReplacer {
	return &LRUReplacer{
		size:      size,
		frame_map: make(map[frame_id_t]int64),
		timestamp: 0,
	}
}

func (replacer *LRUReplacer) Size() int {
	return len(replacer.frame_map)
}

//Select a cache to replace
func (replacer *LRUReplacer) Victim(frame_id *frame_id_t) bool {
	if replacer.Size() == 0 {
		*frame_id = INVALID_FRAME_ID
		return false
	}
	replacer.replacer_mutex.Lock()
	defer replacer.replacer_mutex.Unlock()
	var min_timestamp int64
	min_timestamp = 35432645645
	var min_frame_id frame_id_t
	min_frame_id = INVALID_FRAME_ID
	for frameid, ts := range replacer.frame_map {
		if ts < int64(min_timestamp) {
			min_timestamp = ts
			min_frame_id = frameid
		}
	}
	delete(replacer.frame_map, min_frame_id)
	*frame_id = min_frame_id
	return true
}

func (replacer *LRUReplacer) IntoReplacer(frame_id frame_id_t) error {
	replacer.replacer_mutex.Lock()
	defer replacer.replacer_mutex.Unlock()
	if replacer.Size() > int(replacer.size) {
		return errors.New("lru no space")
	}
	if frame_id >= frame_id_t(replacer.size) {
		return errors.New("frame_id cannot > size")
	}
	if _, ok := replacer.frame_map[frame_id]; ok {
		delete(replacer.frame_map, frame_id)
	}
	replacer.frame_map[frame_id] = replacer.timestamp
	replacer.timestamp++
	return nil
}

// func (replacer *LRUReplacer) Pin(frame_id frame_id_t) {
// 	replacer.replacer_mutex.Lock()
// 	defer replacer.replacer_mutex.Unlock()
// 	delete(replacer.frame_map, frame_id)
// }

// func (replacer *LRUReplacer) Unpin(frame_id frame_id_t) {
// 	replacer.replacer_mutex.Lock()
// 	defer replacer.replacer_mutex.Unlock()
// 	if _, ok := replacer.frame_map[frame_id]; ok {
// 		return
// 	}
// 	replacer.frame_map[frame_id] = replacer.timestamp
// 	replacer.timestamp++
// }
