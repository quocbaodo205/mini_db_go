package main

import "fmt"

type Node any

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

// Define leaf node
type BTreeLeafNode struct {
	nkey   int
	keys   [INTERNAL_MAX_KEY]int
	values [INTERNAL_MAX_KEY]int
}

func NewLNode() BTreeLeafNode {
	var new_keys [INTERNAL_MAX_KEY]int
	var new_vals [INTERNAL_MAX_KEY]int
	return BTreeLeafNode{
		nkey:   0,
		keys:   new_keys,
		values: new_vals,
	}
}

// Find last position so that the key <= find_key
func (node *BTreeLeafNode) FindLastLE(findKey int) int {
	pos := -1
	for i := 0; i < node.nkey; i++ {
		if node.keys[i] <= findKey {
			pos = i
		}
	}
	return pos
}

// Insert a key-children pair into the Internal Node
func (node *BTreeLeafNode) InsertKV(insertKey int, insertValue int) {
	// Find last less or equal as position to insert
	pos := node.FindLastLE(insertKey)
	// [ 1,4,7,| | ] -> insert 3
	// [ 1,| |,4,7 ] -> insert 3
	for i := node.nkey - 1; i > pos; i-- {
		node.keys[i+1] = node.keys[i]
		node.values[i+1] = node.values[i]
	}
	node.keys[pos+1] = insertKey
	node.values[pos+1] = insertValue
	// [ 1,3,4,7 ]
	node.nkey += 1
}

// Split a node into 2 equal part
func (node *BTreeLeafNode) Split() BTreeLeafNode {
	var newKeys [INTERNAL_MAX_KEY]int
	var newValues [INTERNAL_MAX_KEY]int
	// Split in the middle
	pos := node.nkey / 2
	// [ 1 , 2 , 0 , 0 ] -> pos = 2
	// [ 3 , 4 , 0 , 0 ]
	for i := pos; i < node.nkey; i++ {
		newKeys[i-pos] = node.keys[i] // n[0] = o[2]
		newValues[i-pos] = node.values[i]
		node.keys[i] = 0
		node.values[i] = 0
	}
	newNode := BTreeLeafNode{
		nkey:   node.nkey - pos,
		keys:   newKeys,
		values: newValues,
	}
	node.nkey = pos
	return newNode
}

// B+Tree structure
type BPTree struct {
	head Node
}

func NewBPTree() BPTree {
	newINode := NewINode()
	return BPTree{
		head: &newINode,
	}
}

func (tree *BPTree) insertRecursive(node Node, insertKey int, insertValue int) Node {
	// Insert a key value pair.
	// Current: [3] | 3 -> [(3,3), (5,5)]
	if convert, ok := node.(*BTreeInternalNode); ok {
		pos := convert.FindLastLE(insertKey) // -> -1
		if convert.nkey == 0 {
			// Insert in the begining
			firstLeaf := NewLNode()
			firstLeaf.InsertKV(insertKey, insertValue)
			convert.InsertKV(insertKey, &firstLeaf)
		} else {
			// Special process for -1 position
			if pos == -1 {
				pos = 0
			}
			child := convert.children[pos]
			// child -> [(2,2), (3,3), (5,5)]
			// Current: [3] -> [(2,2), (3,3), (5,5)]
			// Node -> any (*BTreeInternalNode / *BTreeLeafNode)
			// Child *Node -> Node
			insertResult := tree.insertRecursive(*child, insertKey, insertValue)
			// Take the first key of children and put as promotion key
			if childConvert, ok := (*child).(*BTreeLeafNode); ok {
				convert.keys[pos] = childConvert.keys[0]
			} else {
				childConvert := (*child).(*BTreeInternalNode)
				convert.keys[pos] = childConvert.keys[0]
			}
			// Current: [2] -> [(2,2), (3,3), (5,5)]
			// If need split, insert back to parent.
			if insertResult != nil {
				if childConvert, ok := insertResult.(*BTreeLeafNode); ok {
					convert.InsertKV(childConvert.keys[0], childConvert)
				} else {
					childConvert := insertResult.(*BTreeInternalNode)
					convert.InsertKV(childConvert.keys[0], childConvert)
				}
			}
			// After insert, check if need split.
			if convert.nkey == INTERNAL_MAX_KEY {
				newInternal := convert.Split()
				return &newInternal
			}
		}
	} else {
		convert := node.(*BTreeLeafNode)
		convert.InsertKV(insertKey, insertValue)

		// After insert, check if need split.
		if convert.nkey == INTERNAL_MAX_KEY {
			newLeaf := convert.Split()
			return &newLeaf
		}
	}

	return nil
}

func (tree *BPTree) Insert(insertKey int, insertValue int) {
	insertResult := tree.insertRecursive(tree.head, insertKey, insertValue)
	if insertResult != nil {
		childConvert := insertResult.(*BTreeInternalNode)
		newHead := NewINode()
		newHead.nkey = 2
		newHead.keys[0] = tree.head.(*BTreeInternalNode).keys[0]
		newHead.keys[1] = childConvert.keys[0]
		newHead.children[0] = tree.head.(*BTreeInternalNode).children[0]
		newHead.children[1] = childConvert.children[0]
		tree.head = &newHead
	}
}

func main() {
	fmt.Println("Hello word")
}
