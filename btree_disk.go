package main

import (
	"bytes"
	"fmt"
	"os"
)

// All constant for easier calculation
const BLOCK_SIZE = 4096
const MAX_KEY_SIZE = 16
const MAX_VAL_SIZE = 32

// Each internal page takes:
// - Header (8 + 64)
// - nkey (16)
// - list of keys: n * (16 + 8*8)
// - list of children: n*64
// => 8 + 64 + 16 + n*(16 + MAX_KEY_SIZE*8) + n*64 <= BLOCK_SIZE
const INTERNAL_MAX_KEY = (BLOCK_SIZE - (8 + 64 + 16)) / (16 + MAX_KEY_SIZE*8 + 64)

// Each leaf page takes:
// - Header (8 + 64)
// - nkey (16)
// - list of kv: n * (16 + 16 + MAX_KEY_SIZE*8 + MAX_VAL_SIZE*8)
const LEAF_MAX_KV = (BLOCK_SIZE - (8 + 64 + 16)) / (16 + 16 + MAX_KEY_SIZE*8 + MAX_VAL_SIZE*8)

// ========================== File Allocator ==========================
type FileAllocator struct {
	last_free  uint64   // start at 1,2,3,4,5,6...
	free_block []uint64 // Always less than last_free
}

var isDebugMode = false

// Always return a pointer on disk to write data to
// <= BLOCK_SIZE bytes -> increase by BLOCK_SIZE
func (a *FileAllocator) alloc() uint64 {
	if len(a.free_block) == 0 {
		ptr := a.last_free * BLOCK_SIZE
		if isDebugMode {
			fmt.Println("allocating block ", ptr/BLOCK_SIZE)
		}
		a.last_free += 1
		return ptr
	}
	ptr := a.free_block[0] * BLOCK_SIZE
	a.free_block = a.free_block[1:]
	if isDebugMode {
		fmt.Println("allocating block ", ptr/BLOCK_SIZE)
	}
	return ptr
}

func (a *FileAllocator) free(ptr uint64) {
	if isDebugMode {
		fmt.Println("freeing block ", ptr/BLOCK_SIZE)
	}
	a.free_block = append(a.free_block, ptr/BLOCK_SIZE)
}

// TODO: Write Allocator to disk
func (a *FileAllocator) writeAllToFile(file *os.File) {}

// TODO: Load allocator from file
func LoadFileAllocator(fileName string) FileAllocator {
	// buffer := new(bytes.Buffer) // Buffer size = 0
	// Step 1: Open file
	file, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	return FileAllocator{}
}

type InsertResult struct {
	node_ptr       uint64
	node_promo_key KeyEntry
	new_node_ptr   uint64 // Need to split, else 0
	new_promo_key  KeyEntry
}

type DelResult struct {
	node_ptr       uint64
	node_promo_key KeyEntry
}

// ========================== B+Tree structure ==========================
type BPTreeDisk struct {
	fileName      string
	fileAllocator FileAllocator
}

// To create new, clear the file and write first 0 header to it.
func NewBPTreeDisk(fileName string) BPTreeDisk {
	// Step 1: Open file with create / truncate
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close() // Persist

	buffer := new(bytes.Buffer) // Buffer size = 0
	metaPage := MetaPage{
		header: PageHeader{
			page_type:         0,
			next_page_pointer: 0,
		},
	}

	metaPage.write_to_buffer(buffer)

	tree := BPTreeDisk{
		fileName: fileName,
		fileAllocator: FileAllocator{
			last_free:  1,
			free_block: []uint64{},
		},
	}
	tree.writeBufferToFileFirst(buffer, file)
	return tree
}

// Reuse buffer style: buffer always of size BLOCK_SIZE
func (tree *BPTreeDisk) readBlockAtPointer(ptr uint64, buffer *bytes.Buffer, file *os.File) {
	inbuf := make([]byte, BLOCK_SIZE)
	sz, err := file.ReadAt(inbuf, int64(ptr))
	if err != nil {
		// Not a eof problem
		if sz == 0 {
			panic(err)
		}
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

func (tree *BPTreeDisk) insertRecursive(node any, insertKey *KeyEntry, insertKV *KeyVal, buffer *bytes.Buffer, file *os.File, deletedPtr *[]uint64) InsertResult {
	// Insert a key value pair.
	// Current: [3] | 3 -> [(3,3), (5,5)]
	if convert, ok := node.(*BTreeInternalPage); ok {
		if isDebugMode {
			fmt.Printf("Internal page = %v\n", *convert)
		}
		pos := convert.FindLastLE(insertKey) // -> -1
		if convert.nkey == 0 {
			// Insert in the begining
			firstLeaf := NewLPage()
			firstLeaf.InsertKV(insertKV)
			buffer.Reset()
			if isDebugMode {
				fmt.Printf("Leaf = %v\n", firstLeaf)
			}
			firstLeaf.write_to_buffer(buffer)
			leafPtr := tree.writeBufferToFile(buffer, file)
			if isDebugMode {
				fmt.Println("Writer to pointer starting at ", leafPtr)
			}
			convert.InsertKV(insertKey, leafPtr)
			if isDebugMode {
				fmt.Printf("After insert, internal page page = %v\n", *convert)
			}
			// Save convert to disk
			buffer.Reset()
			convert.write_to_buffer(buffer)
			oldPtr := tree.writeBufferToFile(buffer, file)
			return InsertResult{
				node_ptr:       oldPtr,
				node_promo_key: convert.keys[0],
				new_node_ptr:   0,
				new_promo_key:  KeyEntry{},
			}
		} else {
			// Special process for -1 position
			if pos == -1 {
				pos = 0
			}
			if isDebugMode {
				fmt.Println("pos = ", pos, ", childptr = ", convert.children[pos])
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
				// fmt.Printf("read ipage = %v", ipage)
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
			insertResult := tree.insertRecursive(childNode, insertKey, insertKV, buffer, file, deletedPtr)
			if isDebugMode {
				fmt.Printf("Child insert result: %v\n", insertResult)
			}
			convert.keys[pos] = insertResult.node_promo_key
			*deletedPtr = append(*deletedPtr, convert.children[pos])
			convert.children[pos] = insertResult.node_ptr
			// Current: [2] -> [(2,2), (3,3), (5,5)]
			// If need split, insert back to parent.
			if insertResult.new_node_ptr != 0 {
				convert.InsertKV(&insertResult.new_promo_key, insertResult.new_node_ptr)
			}
			if isDebugMode {
				fmt.Printf("After insert, internal node = %v\n", *convert)
			}
			// After insert, check if need split.
			if convert.nkey == INTERNAL_MAX_KEY {
				newInternal := convert.Split()
				if isDebugMode {
					fmt.Printf("Need split, old = %v, new = %v\n", *convert, newInternal)
				}
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
		if isDebugMode {
			fmt.Printf("Leaf = %v\n", *convert)
		}
		convert.InsertKV(insertKV)
		if isDebugMode {
			fmt.Printf("Leaf after insert = %v\n", *convert)
		}

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
}

func (tree *BPTreeDisk) Insert(insertKeyBytes []byte, insertValueBytes []byte) {
	buffer := new(bytes.Buffer) // Buffer size = 0
	insertKey := NewKeyEntryFromBytes(insertKeyBytes)
	insertKV := NewKeyValFromBytes(insertKeyBytes, insertValueBytes)
	// Step 1: Open file
	file, err := os.OpenFile(tree.fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close() // Persist
	// Step 2: Read MetaPage
	tree.readBlockAtPointer(0, buffer, file) // Buffer size = BLOCK_SIZE
	metaPage := MetaPage{}
	metaPage.read_from_buffer(buffer) // buffer size decrease
	// fmt.Printf("Meta page: %v\n", metaPage)
	internalPage := NewIPage()
	// Step 2': Read first internal page
	if metaPage.header.next_page_pointer != 0 {
		tree.readBlockAtPointer(metaPage.header.next_page_pointer, buffer, file) // Buffer size = BLOCK_SIZE
		internalPage.read_from_buffer(buffer, true)                              // Buffer size decrease
	}

	deletedPtr := make([]uint64, 0)

	// Step 3: Insert sub structure
	insertResult := tree.insertRecursive(&internalPage, &insertKey, &insertKV, buffer, file, &deletedPtr)
	// fmt.Printf("Insert res: %v\n", insertResult)
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
	if metaPage.header.next_page_pointer != 0 {
		deletedPtr = append(deletedPtr, metaPage.header.next_page_pointer)
	}
	metaPage.header.next_page_pointer = first_internal_page_ptr
	buffer.Reset()
	metaPage.write_to_buffer(buffer)
	tree.writeBufferToFileFirst(buffer, file)
	// Defragment
	for _, x := range deletedPtr {
		tree.fileAllocator.free(x)
	}
}

func (tree *BPTreeDisk) Find(key []byte) *KeyVal {
	buffer := new(bytes.Buffer) // Buffer size = 0
	findKeyE := NewKeyEntryFromBytes(key)
	var emptyVal []byte = make([]byte, 0)
	findKeyV := NewKeyValFromBytes(key, emptyVal)
	// Step 1: Open file
	file, err := os.OpenFile(tree.fileName, os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close() // Persist
	// Step 2: Read MetaPage
	tree.readBlockAtPointer(0, buffer, file) // Buffer size = BLOCK_SIZE
	metaPage := MetaPage{}
	metaPage.read_from_buffer(buffer) // buffer size decrease

	internalPage := BTreeInternalPage{}
	// Step 2': Read first internal page
	if metaPage.header.next_page_pointer != 0 {
		tree.readBlockAtPointer(metaPage.header.next_page_pointer, buffer, file) // Buffer size = BLOCK_SIZE
		internalPage.read_from_buffer(buffer, true)                              // Buffer size decrease
	}

	var node any
	node = &internalPage

	for {
		if convert, ok := node.(*BTreeInternalPage); ok {
			// fmt.Printf("internal page: %v\n", *convert)
			pos := convert.FindLastLE(&findKeyE)
			// fmt.Println("pos = ", pos)
			if pos == -1 {
				return nil
			}
			child := convert.children[pos]
			buffer.Reset()
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
			node = childNode
		} else {
			convert := node.(*BTreeLeafPage)
			// fmt.Printf("leaf page: %v\n", *convert)
			pos := convert.FindLastLE(&findKeyV)
			// fmt.Println("pos = ", pos)
			if pos == -1 {
				return nil
			}
			foundKV := convert.kv[pos]

			if foundKV.compare(&findKeyV) == 0 {
				return &foundKV
			}
			return nil
		}
	}
}

// Assume key can be found always
func (tree *BPTreeDisk) setRecursive(node any, setKey *KeyEntry, setKV *KeyVal, buffer *bytes.Buffer, file *os.File, deletedPtr *[]uint64) InsertResult {
	// Insert a key value pair.
	// Current: [3] | 3 -> [(3,3), (5,5)]
	if convert, ok := node.(*BTreeInternalPage); ok {
		if isDebugMode {
			fmt.Printf("set internal page: %v\n", *convert)
		}
		pos := convert.FindLastLE(setKey) // -> always have
		if isDebugMode {
			fmt.Printf("pos = %v\n", pos)
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
		setResult := tree.setRecursive(childNode, setKey, setKV, buffer, file, deletedPtr)
		if isDebugMode {
			fmt.Printf("Set result: %v\n", setResult)
		}
		*deletedPtr = append(*deletedPtr, convert.children[pos])
		convert.children[pos] = setResult.node_ptr
		if isDebugMode {
			fmt.Printf("set internal page after set: %v\n", *convert)
		}
		// Current: [2] -> [(2,2), (3,3), (5,5)]
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
	} else {
		convert := node.(*BTreeLeafPage)
		if isDebugMode {
			fmt.Printf("set leaf page: %v\n", *convert)
		}
		pos := convert.FindLastLE(setKV)
		if isDebugMode {
			fmt.Printf("pos = %v\n", pos)
		}
		convert.kv[pos] = *setKV // Set it as the new key value
		if isDebugMode {
			fmt.Printf("set leaf page after set: %v\n", *convert)
		}
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

func (tree *BPTreeDisk) Set(setKeyBytes []byte, setValueBytes []byte) {
	findRes := tree.Find(setKeyBytes)
	if findRes == nil {
		if isDebugMode {
			fmt.Printf("key %v not found, inserting...\n", setKeyBytes)
		}
		tree.Insert(setKeyBytes, setValueBytes)
		return
	}

	buffer := new(bytes.Buffer) // Buffer size = 0
	setKey := NewKeyEntryFromBytes(setKeyBytes)
	setKV := NewKeyValFromBytes(setKeyBytes, setValueBytes)
	if isDebugMode {
		fmt.Printf("Set kv = %v\n", setKV)
	}
	// Step 1: Open file
	file, err := os.OpenFile(tree.fileName, os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close() // Persist
	// Step 2: Read MetaPage
	tree.readBlockAtPointer(0, buffer, file) // Buffer size = BLOCK_SIZE
	metaPage := MetaPage{}
	metaPage.read_from_buffer(buffer) // buffer size decrease

	internalPage := NewIPage()
	// Step 2': Read first internal page
	if metaPage.header.next_page_pointer != 0 {
		tree.readBlockAtPointer(metaPage.header.next_page_pointer, buffer, file) // Buffer size = BLOCK_SIZE
		internalPage.read_from_buffer(buffer, true)                              // Buffer size decrease
	}
	if isDebugMode {
		fmt.Printf("First internal page: %v\n", internalPage)
	}
	deletedPtr := make([]uint64, 0)

	// Step 3: Set sub structure
	setResult := tree.setRecursive(&internalPage, &setKey, &setKV, buffer, file, &deletedPtr)
	if isDebugMode {
		fmt.Printf("Set result: %v\n", setResult)
	}
	// Step 4: Modify MetaPage and save to disk
	first_internal_page_ptr := setResult.node_ptr
	// Assume last step has the first internal page ptr
	if metaPage.header.next_page_pointer != 0 {
		deletedPtr = append(deletedPtr, metaPage.header.next_page_pointer)
	}
	metaPage.header.next_page_pointer = first_internal_page_ptr
	buffer.Reset()
	metaPage.write_to_buffer(buffer)
	tree.writeBufferToFileFirst(buffer, file)
	// Defragment
	for _, x := range deletedPtr {
		tree.fileAllocator.free(x)
	}
}

// Assume value always have
func (tree *BPTreeDisk) delRecursive(node any, delKey *KeyEntry, delKV *KeyVal, buffer *bytes.Buffer, file *os.File, deletedPtr *[]uint64) DelResult {
	// Insert a key value pair.
	// Current: [3] | 3 -> [(3,3), (5,5)]
	if convert, ok := node.(*BTreeInternalPage); ok {
		pos := convert.FindLastLE(delKey) // -> always have
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
		delResult := tree.delRecursive(childNode, delKey, delKV, buffer, file, deletedPtr)
		if delResult.node_ptr == 0 {
			// Whole child got deleted
			*deletedPtr = append(*deletedPtr, convert.children[pos])
			convert.DelKVAtPos(pos)
			if convert.nkey == 0 {
				return DelResult{
					node_ptr:       0,
					node_promo_key: KeyEntry{},
				}
			}
		} else {
			convert.keys[pos] = delResult.node_promo_key
			*deletedPtr = append(*deletedPtr, convert.children[pos])
			convert.children[pos] = delResult.node_ptr
		}
		// Current: [2] -> [(2,2), (3,3), (5,5)]
		// Save current page
		buffer.Reset()
		convert.write_to_buffer(buffer)
		oldPtr := tree.writeBufferToFile(buffer, file)
		return DelResult{
			node_ptr:       oldPtr,
			node_promo_key: convert.keys[0],
		}
	} else {
		convert := node.(*BTreeLeafPage)
		convert.DelKV(delKV)
		if convert.nkv == 0 {
			return DelResult{
				node_ptr:       0,
				node_promo_key: KeyEntry{},
			}
		}

		// Save current page
		buffer.Reset()
		convert.write_to_buffer(buffer)
		oldPtr := tree.writeBufferToFile(buffer, file)
		return DelResult{
			node_ptr:       oldPtr,
			node_promo_key: getKeyEntryFromKeyVal(&convert.kv[0]),
		}
	}
}

func (tree *BPTreeDisk) Del(key []byte) bool {
	findRes := tree.Find(key)
	if findRes == nil {
		return false
	}

	buffer := new(bytes.Buffer) // Buffer size = 0
	delKeyE := NewKeyEntryFromBytes(key)
	var emptyVal []byte = make([]byte, 0)
	delKeyV := NewKeyValFromBytes(key, emptyVal)

	// Step 1: Open file
	file, err := os.OpenFile(tree.fileName, os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close() // Persist
	// Step 2: Read MetaPage
	tree.readBlockAtPointer(0, buffer, file) // Buffer size = BLOCK_SIZE
	metaPage := MetaPage{}
	metaPage.read_from_buffer(buffer) // buffer size decrease

	internalPage := NewIPage()
	// Step 2': Read first internal page
	if metaPage.header.next_page_pointer != 0 {
		tree.readBlockAtPointer(metaPage.header.next_page_pointer, buffer, file) // Buffer size = BLOCK_SIZE
		internalPage.read_from_buffer(buffer, true)                              // Buffer size decrease
	}
	deletedPtr := make([]uint64, 0)

	// Step 3: Insert sub structure
	delResult := tree.delRecursive(&internalPage, &delKeyE, &delKeyV, buffer, file, &deletedPtr)
	// Step 4: Modify MetaPage and save to disk
	first_internal_page_ptr := delResult.node_ptr
	// Assume last step has the first internal page ptr
	if metaPage.header.next_page_pointer != 0 {
		deletedPtr = append(deletedPtr, metaPage.header.next_page_pointer)
	}
	metaPage.header.next_page_pointer = first_internal_page_ptr
	buffer.Reset()
	metaPage.write_to_buffer(buffer)
	tree.writeBufferToFileFirst(buffer, file)
	// Defragment
	for _, x := range deletedPtr {
		tree.fileAllocator.free(x)
	}
	return true
}
