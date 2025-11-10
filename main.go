package main

import "fmt"

const INTERNAL_MAX_KEY = 4

type Node interface{}

type BTreeInternalNode struct {
	nkey     int
	keys     [INTERNAL_MAX_KEY]int
	children [INTERNAL_MAX_KEY]*Node
}

func NewINode() BTreeInternalNode {
	var new_keys [INTERNAL_MAX_KEY]int
	var new_children [INTERNAL_MAX_KEY]*Node
	return BTreeInternalNode{
		nkey:     0,
		keys:     new_keys,
		children: new_children,
	}
}

// Find last position so that the key <= find_key
func (node *BTreeInternalNode) FindLastLE(findKey int) int {
	pos := -1
	for i := 0; i < node.nkey; i++ {
		if node.keys[i] <= findKey {
			pos = i
		}
	}
	return pos
}

// Insert a key-children pair into the Internal Node
func (node *BTreeInternalNode) InsertKV(insertKey int, insertChild Node) {
	// Find last less or equal as position to insert
	pos := node.FindLastLE(insertKey)
	// [ 1,4,7,| | ] -> insert 3
	// [ 1,| |,4,7 ] -> insert 3
	for i := node.nkey - 1; i > pos; i-- {
		node.keys[i+1] = node.keys[i]
		node.children[i+1] = node.children[i]
	}
	node.keys[pos+1] = insertKey
	node.children[pos+1] = &insertChild
	// [ 1,3,4,7 ]
	node.nkey += 1
}

// Split a node into 2 equal part
func (node *BTreeInternalNode) Split() BTreeInternalNode {
	var newKeys [INTERNAL_MAX_KEY]int
	var newChildren [INTERNAL_MAX_KEY]*Node
	// Split in the middle
	pos := node.nkey / 2
	// [ 1 , 2 , 0 , 0 ] -> pos = 2
	// [ 3 , 4 , 0 , 0 ]
	for i := pos; i < node.nkey; i++ {
		newKeys[i-pos] = node.keys[i] // n[0] = o[2]
		newChildren[i-pos] = node.children[i]
		node.keys[i] = 0
		node.children[i] = nil
	}
	newNode := BTreeInternalNode{
		nkey:     node.nkey - pos,
		keys:     newKeys,
		children: newChildren,
	}
	node.nkey = pos
	return newNode
}

func main() {
	fmt.Println("Hello word")
}
