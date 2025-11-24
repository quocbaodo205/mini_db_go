package main

// B+Tree structure
type BPTreeDisk struct {
	head MetaPage
}

func NewBPTreeDisk() BPTreeDisk {
	return BPTreeDisk{
		head: MetaPage{
			header: PageHeader{
				page_type:         0,
				next_page_pointer: 0,
			},
		},
	}
}

type InsertResult struct {
	node_ptr       uint64
	node_promo_key KeyEntry
	new_node_ptr   uint64 // Need to split, else 0
	new_promo_key  KeyEntry
}

func (tree *BPTreeDisk) insertRecursive(page_ptr uint64, insertKey KeyEntry, insertValue int) InsertResult {
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

func (tree *BPTreeDisk) Insert(insertKey int, insertValue int) {
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
