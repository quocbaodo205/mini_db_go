package main

import (
	"testing"
)

func TestDatabase(t *testing.T) {
	node := NewINode()
	c := NewINode()
	node.InsertKV(3, c)
	// [3]
	if node.nkey != 1 {
		t.Errorf("got nkey = %v, expect %v", node.nkey, 1)
	}
	if node.keys[0] != 3 {
		t.Errorf("got key[0] = %v, expect %v", node.keys[0], 3)
	}
	node.InsertKV(10, c)
	// [3, 10]
	if node.nkey != 2 {
		t.Errorf("got nkey = %v, expect %v", node.nkey, 2)
	}
	if node.keys[0] != 3 {
		t.Errorf("got key[0] = %v, expect %v", node.keys[0], 3)
	}
	if node.keys[1] != 10 {
		t.Errorf("got key[1] = %v, expect %v", node.keys[1], 10)
	}
	node.InsertKV(5, c)
	// [3, 5, 10]
	if node.nkey != 3 {
		t.Errorf("got nkey = %v, expect %v", node.nkey, 3)
	}
	if node.keys[0] != 3 {
		t.Errorf("got key[0] = %v, expect %v", node.keys[0], 3)
	}
	if node.keys[1] != 5 {
		t.Errorf("got key[1] = %v, expect %v", node.keys[1], 5)
	}
	if node.keys[2] != 10 {
		t.Errorf("got key[2] = %v, expect %v", node.keys[2], 10)
	}
	node.InsertKV(12, c)
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
	if node.keys[0] != 3 {
		t.Errorf("got key[0] = %v, expect %v", node.keys[0], 3)
	}
	if node.keys[1] != 5 {
		t.Errorf("got key[1] = %v, expect %v", node.keys[1], 5)
	}
	if newNode.keys[0] != 10 {
		t.Errorf("got key[2] = %v, expect %v", newNode.keys[0], 10)
	}
	if newNode.keys[1] != 12 {
		t.Errorf("got key[2] = %v, expect %v", newNode.keys[1], 10)
	}
	// if d, ok := (*node.children[0]).(BTreeInternalNode); ok {
	// }
}
