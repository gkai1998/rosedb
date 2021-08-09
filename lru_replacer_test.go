package rosedb

import (
	"testing"
)

func Test_lru_algorithm1(t *testing.T) {
	lru_replacer := New(7)
	var err error
	err = lru_replacer.IntoReplacer(1)
	if err != nil {
		t.Errorf("intoreplacer returns a error")
	}
	err = lru_replacer.IntoReplacer(2)
	if err != nil {
		t.Errorf("intoreplacer returns a error")
	}
	err = lru_replacer.IntoReplacer(3)
	if err != nil {
		t.Errorf("intoreplacer returns a error")
	}
	err = lru_replacer.IntoReplacer(4)
	if err != nil {
		t.Errorf("intoreplacer returns a error")
	}
	err = lru_replacer.IntoReplacer(5)
	if err != nil {
		t.Errorf("intoreplacer returns a error")
	}
	err = lru_replacer.IntoReplacer(6)
	if err != nil {
		t.Errorf("intoreplacer returns a error")
	}
	err = lru_replacer.IntoReplacer(1)
	if err != nil {
		t.Errorf("intoreplacer returns a error")
	}
	if lru_replacer.Size() != 6 {
		t.Errorf("lru size %d,want %d", lru_replacer.Size(), 6)
	}
	var value frame_id_t
	lru_replacer.Victim(&value)
	if value != 2 {
		t.Errorf("value is %d,want %d", value, 2)
	}
	lru_replacer.Victim(&value)
	if value != 3 {
		t.Errorf("value is %d,want %d", value, 3)
	}
	lru_replacer.Victim(&value)
	if value != 4 {
		t.Errorf("value is %d,want %d", value, 4)
	}
	if lru_replacer.Size() != 3 {
		t.Errorf("lru size %d,want %d", lru_replacer.Size(), 3)
	}
	lru_replacer.IntoReplacer(5)
	lru_replacer.Victim(&value)
	if value != 6 {
		t.Errorf("value is %d,want %d", value, 6)
	}
	lru_replacer.Victim(&value)
	if value != 1 {
		t.Errorf("value is %d,want %d", value, 1)
	}
	lru_replacer.Victim(&value)
	if value != 5 {
		t.Errorf("value is %d,want %d", value, 5)
	}
	if lru_replacer.Size() != 0 {
		t.Errorf("lru size %d,want %d", lru_replacer.Size(), 0)
	}
}

func Test_lru_algorithm2(t *testing.T) {
	lru_replacer := New(7)
	var err error
	err = lru_replacer.IntoReplacer(0)
	if err != nil {
		t.Errorf("intoreplacer returns a error")
	}
	err = lru_replacer.IntoReplacer(1)
	if err != nil {
		t.Errorf("intoreplacer returns a error")
	}
	err = lru_replacer.IntoReplacer(2)
	if err != nil {
		t.Errorf("intoreplacer returns a error")
	}
	err = lru_replacer.IntoReplacer(3)
	if err != nil {
		t.Errorf("intoreplacer returns a error")
	}
	err = lru_replacer.IntoReplacer(4)
	if err != nil {
		t.Errorf("intoreplacer returns a error")
	}
	err = lru_replacer.IntoReplacer(5)
	if err != nil {
		t.Errorf("intoreplacer returns a error")
	}
	err = lru_replacer.IntoReplacer(6)
	if err != nil {
		t.Errorf("intoreplacer returns a error")
	}
	err = lru_replacer.IntoReplacer(8)
	if err == nil {
		t.Errorf("intoreplacer returns a error")
	}
	var value frame_id_t
	lru_replacer.Victim(&value)
	lru_replacer.Victim(&value)
	err = lru_replacer.IntoReplacer(10)
	if err == nil {
		t.Errorf("intoreplacer returns a error")
	}
}
