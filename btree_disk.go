package main

import (
	"bytes"
	"os"
)

// ========================== File Allocator ==========================
type FileAllocator struct {
	last_pointer uint64
}

// Always return a pointer on disk to write data to
// <= 4096 bytes -> increase by 4096
func (a *FileAllocator) alloc() uint64 {
	old_pointer := a.last_pointer
	a.last_pointer += 4096
	return old_pointer
}

// TODO: Free to reuse memory

type InsertResult struct {
	node_ptr       uint64
	node_promo_key KeyEntry
	new_node_ptr   uint64 // Need to split, else 0
	new_promo_key  KeyEntry
}

// ========================== B+Tree structure ==========================
type BPTreeDisk struct {
	fileName      string
	fileAllocator FileAllocator
}

func NewBPTreeDisk(fileName string) BPTreeDisk {
	return BPTreeDisk{
		fileName: fileName,
		fileAllocator: FileAllocator{
			last_pointer: 4096,
		},
	}
}

// Reuse buffer style: buffer always of size 4096
func (tree *BPTreeDisk) readBlockAtPointer(ptr uint64, buffer *bytes.Buffer, file *os.File) {
	inbuf := make([]byte, 4096)
	_, err := file.ReadAt(inbuf, int64(ptr))
	if err != nil {
		panic(err)
	}
	buffer.Reset()
	buffer.Write(inbuf)
}

// Return a disk pointer to this data
func (tree *BPTreeDisk) writeBufferToFile(buffer *bytes.Buffer, file *os.File) uint64 {
	last_ptr := tree.fileAllocator.alloc()
	_, err := file.WriteAt(buffer.Bytes(), int64(last_ptr))
	if err != nil {
		panic(err)
	}
	return last_ptr
}

func (tree *BPTreeDisk) writeBufferToFileFirst(buffer *bytes.Buffer, file *os.File) {
	_, err := file.WriteAt(buffer.Bytes(), 0)
	if err != nil {
		panic(err)
	}
}

func getKeyEntryFromKeyVal(kv *KeyVal) KeyEntry {
	return KeyEntry{
		len:  kv.keylen,
		data: kv.key,
	}
}

func (tree *BPTreeDisk) insertRecursive(node any, insertKey *KeyEntry, insertKV *KeyVal, buffer *bytes.Buffer, file *os.File) InsertResult {
	// Insert a key value pair.
	// Current: [3] | 3 -> [(3,3), (5,5)]
	if convert, ok := node.(*BTreeInternalPage); ok {
		pos := convert.FindLastLE(insertKey) // -> -1
		if convert.nkey == 0 {
			// Insert in the begining
			firstLeaf := NewLPage()
			firstLeaf.InsertKV(insertKV)
			buffer.Reset()
			firstLeaf.write_to_buffer(buffer)
			leafPtr := tree.writeBufferToFile(buffer, file)
			convert.InsertKV(insertKey, leafPtr)
		} else {
			// Special process for -1 position
			if pos == -1 {
				pos = 0
			}
			child := convert.children[pos]
			tree.readBlockAtPointer(child, buffer, file)
			// Try to convert back to either leaf or internal
			header := PageHeader{}
			header.read_from_buffer(buffer)
			var childNode any
			if header.page_type == 1 {
				// Internal page
				ipage := BTreeInternalPage{header: header}
				ipage.read_from_buffer(buffer, false)
				childNode = &ipage
			} else {
				// Leaf page
				lpage := BTreeLeafPage{header: header}
				lpage.read_from_buffer(buffer, false)
				childNode = &lpage
			}
			// child -> [(2,2), (3,3), (5,5)]
			// Current: [3] -> [(2,2), (3,3), (5,5)]
			// Node -> any (*BTreeInternalNode / *BTreeLeafNode)
			// Child *Node -> Node
			insertResult := tree.insertRecursive(childNode, insertKey, insertKV, buffer, file)
			convert.keys[pos] = insertResult.node_promo_key
			convert.children[pos] = insertResult.node_ptr
			// Current: [2] -> [(2,2), (3,3), (5,5)]
			// If need split, insert back to parent.
			if insertResult.new_node_ptr != 0 {
				convert.InsertKV(&insertResult.new_promo_key, insertResult.new_node_ptr)
			}
			// After insert, check if need split.
			if convert.nkey == INTERNAL_MAX_KEY {
				newInternal := convert.Split()
				// Save current page
				buffer.Reset()
				convert.write_to_buffer(buffer)
				oldPtr := tree.writeBufferToFile(buffer, file)
				// Save new page
				buffer.Reset()
				newInternal.write_to_buffer(buffer)
				newPtr := tree.writeBufferToFile(buffer, file)
				return InsertResult{
					node_ptr:       oldPtr,
					node_promo_key: convert.keys[0],
					new_node_ptr:   newPtr,
					new_promo_key:  newInternal.keys[0],
				}
			} else {
				// Save current page
				buffer.Reset()
				convert.write_to_buffer(buffer)
				oldPtr := tree.writeBufferToFile(buffer, file)
				return InsertResult{
					node_ptr:       oldPtr,
					node_promo_key: convert.keys[0],
					new_node_ptr:   0,
					new_promo_key:  KeyEntry{},
				}
			}
		}
	} else {
		convert := node.(*BTreeLeafPage)
		convert.InsertKV(insertKV)

		// After insert, check if need split.
		if convert.nkv == LEAF_MAX_KV {
			newLeaf := convert.Split()
			// Save current page
			buffer.Reset()
			convert.write_to_buffer(buffer)
			oldPtr := tree.writeBufferToFile(buffer, file)
			// Save new page
			buffer.Reset()
			newLeaf.write_to_buffer(buffer)
			newPtr := tree.writeBufferToFile(buffer, file)
			return InsertResult{
				node_ptr:       oldPtr,
				node_promo_key: getKeyEntryFromKeyVal(&convert.kv[0]),
				new_node_ptr:   newPtr,
				new_promo_key:  getKeyEntryFromKeyVal(&newLeaf.kv[0]),
			}
		} else {
			// Save current page
			buffer.Reset()
			convert.write_to_buffer(buffer)
			oldPtr := tree.writeBufferToFile(buffer, file)
			return InsertResult{
				node_ptr:       oldPtr,
				node_promo_key: getKeyEntryFromKeyVal(&convert.kv[0]),
				new_node_ptr:   0,
				new_promo_key:  KeyEntry{},
			}
		}
	}

	return InsertResult{}
}

// Test: Keep key / val as int
func (tree *BPTreeDisk) Insert(insertKeyInt int, insertValueInt int) {
	buffer := new(bytes.Buffer) // Buffer size = 0
	insertKey := NewKeyEntryFromInt(int64(insertKeyInt))
	insertKV := NewKeyValFromInt(int64(insertKeyInt), int64(insertValueInt))
	// Step 1: Open file
	file, err := os.OpenFile(tree.fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close() // Persist
	// Step 2: Read MetaPage
	tree.readBlockAtPointer(0, buffer, file) // Buffer size = 4096
	metaPage := MetaPage{}
	metaPage.read_from_buffer(buffer) // buffer size decrease

	internalPage := BTreeInternalPage{}
	// Step 2': Read first internal page
	if metaPage.header.next_page_pointer != 0 {
		tree.readBlockAtPointer(metaPage.header.next_page_pointer, buffer, file) // Buffer size = 4096
		internalPage.read_from_buffer(buffer, true)                              // Buffer size decrease
	}

	// Step 3: Insert sub structure
	insertResult := tree.insertRecursive(&internalPage, &insertKey, &insertKV, buffer, file)
	// Step 4: Modify MetaPage and save to disk
	var first_internal_page_ptr uint64
	if insertResult.new_node_ptr != 0 {
		// Insert a new page
		newFirstIPage := NewIPage()
		newFirstIPage.nkey = 2
		newFirstIPage.keys[0] = insertResult.node_promo_key
		newFirstIPage.children[0] = insertResult.node_ptr
		newFirstIPage.keys[1] = insertResult.new_promo_key
		newFirstIPage.children[1] = insertResult.new_node_ptr
		buffer.Reset()
		newFirstIPage.write_to_buffer(buffer)
		first_internal_page_ptr = tree.writeBufferToFile(buffer, file)
	} else {
		first_internal_page_ptr = insertResult.node_ptr
	}
	// Assume last step has the first internal page ptr
	metaPage.header.next_page_pointer = first_internal_page_ptr
	buffer.Reset()
	metaPage.write_to_buffer(buffer)
	tree.writeBufferToFileFirst(buffer, file)
}

func (tree *BPTreeDisk) Find(findKey int) *KeyVal {
	buffer := new(bytes.Buffer) // Buffer size = 0
	insertKey := NewKeyEntryFromInt(int64(findKey))
	// Step 1: Open file
	file, err := os.OpenFile(tree.fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close() // Persist
	// Step 2: Read MetaPage
	tree.readBlockAtPointer(0, buffer, file) // Buffer size = 4096
	metaPage := MetaPage{}
	metaPage.read_from_buffer(buffer) // buffer size decrease

	internalPage := BTreeInternalPage{}
	// Step 2': Read first internal page
	if metaPage.header.next_page_pointer != 0 {
		tree.readBlockAtPointer(metaPage.header.next_page_pointer, buffer, file) // Buffer size = 4096
		internalPage.read_from_buffer(buffer, true)                              // Buffer size decrease
	}

	var node any
	node = &internalPage

	for {
		// TODO: implement find in style of while loop
		if convert, ok := node.(*BTreeInternalPage); ok {
		} else {
			convert := node.(*BTreeLeafPage)
		}
	}

	return nil
}
