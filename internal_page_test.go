package main

import (
	"bytes"
	"testing"
)

func TestInternalPage(t *testing.T) {
	node := NewIPage()
	var c uint64 = 0
	key_3 := NewKeyEntryFromInt(3)
	node.InsertKV(key_3, c)
	// [3]
	if node.nkey != 1 {
		t.Errorf("got nkey = %v, expect %v", node.nkey, 1)
	}
	if node.keys[0].compare(&key_3) != 0 {
		t.Errorf("got key[0] = %v, expect %v", node.keys[0], 3)
	}
	key_10 := NewKeyEntryFromInt(10)
	node.InsertKV(key_10, c)
	// [3, 10]
	if node.nkey != 2 {
		t.Errorf("got nkey = %v, expect %v", node.nkey, 2)
	}
	if node.keys[0].compare(&key_3) != 0 {
		t.Errorf("got key[0] = %v, expect %v", node.keys[0], 3)
	}
	if node.keys[1].compare(&key_10) != 0 {
		t.Errorf("got key[1] = %v, expect %v", node.keys[1], 10)
	}
	key_5 := NewKeyEntryFromInt(5)
	node.InsertKV(key_5, c)
	// [3, 5, 10]
	if node.nkey != 3 {
		t.Errorf("got nkey = %v, expect %v", node.nkey, 3)
	}
	if node.keys[0].compare(&key_3) != 0 {
		t.Errorf("got key[0] = %v, expect %v", node.keys[0], 3)
	}
	if node.keys[1].compare(&key_5) != 0 {
		t.Errorf("got key[1] = %v, expect %v", node.keys[1], 5)
	}
	if node.keys[2].compare(&key_10) != 0 {
		t.Errorf("got key[2] = %v, expect %v", node.keys[2], 10)
	}
	key_12 := NewKeyEntryFromInt(12)
	node.InsertKV(key_12, c)
	if node.nkey != 4 {
		t.Errorf("got nkey = %v, expect %v", node.nkey, 4)
	}
	// [3, 5, 10, 12]
	newNode := node.Split()
	// [3, 5] [10, 12]
	if node.nkey != 2 {
		t.Errorf("got nkey = %v, expect %v", node.nkey, 2)
	}
	if newNode.nkey != 2 {
		t.Errorf("got nkey = %v, expect %v", newNode.nkey, 2)
	}
	if node.keys[0].compare(&key_3) != 0 {
		t.Errorf("got key[0] = %v, expect %v", node.keys[0], 3)
	}
	if node.keys[1].compare(&key_5) != 0 {
		t.Errorf("got key[1] = %v, expect %v", node.keys[1], 5)
	}
	if newNode.keys[0].compare(&key_10) != 0 {
		t.Errorf("got key[0] = %v, expect %v", newNode.keys[0], 10)
	}
	if newNode.keys[1].compare(&key_12) != 0 {
		t.Errorf("got key[1] = %v, expect %v", newNode.keys[1], 12)
	}

	buf := new(bytes.Buffer)
	node.write_to_buffer(buf)

	clonedNode := NewIPage()
	clonedNode.read_from_buffer(buf)
	if clonedNode.nkey != 2 {
		t.Errorf("got nkey = %v, expect %v", clonedNode.nkey, 2)
	}
	if clonedNode.keys[0].compare(&key_3) != 0 {
		t.Errorf("got key[0] = %v, expect %v", clonedNode.keys[0], 3)
	}
	if clonedNode.keys[1].compare(&key_5) != 0 {
		t.Errorf("got key[1] = %v, expect %v", clonedNode.keys[1], 5)
	}
}
