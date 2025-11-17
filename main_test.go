package main

import (
	"testing"
)

func TestINode(t *testing.T) {
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
}

func TestLNode(t *testing.T) {
	node := NewLNode()
	node.InsertKV(3, 1)
	// [3]
	if node.nkey != 1 {
		t.Errorf("got nkey = %v, expect %v", node.nkey, 1)
	}
	if node.keys[0] != 3 {
		t.Errorf("got key[0] = %v, expect %v", node.keys[0], 3)
	}
	node.InsertKV(10, 1)
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
	node.InsertKV(5, 1)
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
	node.InsertKV(12, 1)
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
}

func TestBTree(t *testing.T) {
	tree := NewBPTree()
	// head = inode[]
	tree.Insert(3, 3)
	// head = inode[3] , 3 -> lnode[(3,3)]
	if tree.head.(*BTreeInternalNode).keys[0] != 3 {
		t.Errorf("got key[0] = %v, expect %v", tree.head.(BTreeInternalNode).keys[0], 3)
	}
	child := tree.head.(*BTreeInternalNode).children[0]
	if (*child).(*BTreeLeafNode).keys[0] != 3 {
		t.Errorf("got key[0] = %v, expect %v", (*child).(*BTreeLeafNode).keys[0], 3)
	}
	if (*child).(*BTreeLeafNode).values[0] != 3 {
		t.Errorf("got values[0] = %v, expect %v", (*child).(*BTreeLeafNode).values[0], 3)
	}

	// head = inode[3] , 3 -> lnode[(3,3)]
	tree.Insert(5, 5)
	// head = inode[3] , 3 -> lnode[(3,3), (5,5)]
	if tree.head.(*BTreeInternalNode).keys[0] != 3 {
		t.Errorf("got key[0] = %v, expect %v", tree.head.(BTreeInternalNode).keys[0], 3)
	}
	child = tree.head.(*BTreeInternalNode).children[0]
	if (*child).(*BTreeLeafNode).nkey != 2 {
		t.Errorf("got nkey = %v, expect %v", (*child).(*BTreeLeafNode).nkey, 2)

	}
	if (*child).(*BTreeLeafNode).keys[0] != 3 {
		t.Errorf("got key[0] = %v, expect %v", (*child).(*BTreeLeafNode).keys[0], 3)
	}
	if (*child).(*BTreeLeafNode).values[0] != 3 {
		t.Errorf("got values[0] = %v, expect %v", (*child).(*BTreeLeafNode).values[0], 3)
	}
	if (*child).(*BTreeLeafNode).keys[1] != 5 {
		t.Errorf("got key[1] = %v, expect %v", (*child).(*BTreeLeafNode).keys[1], 5)
	}
	if (*child).(*BTreeLeafNode).values[1] != 5 {
		t.Errorf("got values[1] = %v, expect %v", (*child).(*BTreeLeafNode).values[1], 5)
	}

	// head = inode[3] , 3 -> lnode[(3,3), (5,5)]
	tree.Insert(2, 2)
	// head = inode[2] , 2 -> lnode[(2,2), (3,3), (5,5)]
	child = tree.head.(*BTreeInternalNode).children[0]
	if (*child).(*BTreeLeafNode).nkey != 3 {
		t.Errorf("got nkey = %v, expect %v", (*child).(*BTreeLeafNode).nkey, 3)
	}
	if (*child).(*BTreeLeafNode).keys[0] != 2 {
		t.Errorf("got key[0] = %v, expect %v", (*child).(*BTreeLeafNode).keys[0], 2)
	}
	if (*child).(*BTreeLeafNode).keys[1] != 3 {
		t.Errorf("got key[1] = %v, expect %v", (*child).(*BTreeLeafNode).keys[1], 3)
	}
	if (*child).(*BTreeLeafNode).keys[2] != 5 {
		t.Errorf("got key[2] = %v, expect %v", (*child).(*BTreeLeafNode).keys[2], 5)
	}

	// head = inode[2] , 2 -> lnode[(2,2), (3,3), (5,5)]
	tree.Insert(8, 8)
	// head = inode[2] , 2 -> lnode[(2,2), (3,3), (5,5), (8,8)]
	// head = inode[2,5], 2 -> lnode[(2,2),(3,3)], 5 -> lnode[(5,5),(8,8)]
	if tree.head.(*BTreeInternalNode).nkey != 2 {
		t.Errorf("got nkey = %v, expect %v", tree.head.(*BTreeInternalNode).nkey, 2)
	}
	if tree.head.(*BTreeInternalNode).keys[0] != 2 {
		t.Errorf("got keys[0] = %v, expect %v", tree.head.(*BTreeInternalNode).keys[0], 2)
	}
	if tree.head.(*BTreeInternalNode).keys[1] != 5 {
		t.Errorf("got keys[0] = %v, expect %v", tree.head.(*BTreeInternalNode).keys[1], 5)
	}
	child = tree.head.(*BTreeInternalNode).children[1]
	// lnode[(5,5),(8,8)]
	if (*child).(*BTreeLeafNode).nkey != 2 {
		t.Errorf("got nkey = %v, expect %v", (*child).(*BTreeLeafNode).nkey, 2)
	}
	if (*child).(*BTreeLeafNode).keys[0] != 5 {
		t.Errorf("got key[0] = %v, expect %v", (*child).(*BTreeLeafNode).keys[0], 5)
	}
	if (*child).(*BTreeLeafNode).keys[1] != 8 {
		t.Errorf("got key[1] = %v, expect %v", (*child).(*BTreeLeafNode).keys[1], 8)
	}
}

func TestBTreeBig(t *testing.T) {
	// Renew test
	tree := NewBPTree()
	for i := range 100 {
		tree.Insert(i, i)
	}

}
