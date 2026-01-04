package main

import (
	"bytes"
	"os"
)

type PathData struct {
	node     any
	position int
}

type BIter struct {
	path []PathData
	tree *BPTreeDisk
	file *os.File
}

// Get: Do not convert size [0 0 0 0 1 2 3 54 ...]
func (i *BIter) Deref() KeyVal {
	pd := i.path[len(i.path)-1]
	lastNode := pd.node
	// Last node has to be a leaf
	convert := lastNode.(*BTreeLeafPage)
	// fmt.Println("Position = ", pd.position, ", node = ", convert, ", path len = ", len(i.path))
	kv := convert.kv[pd.position]
	return kv
}

func (i *BIter) Next() {
	for {
		if len(i.path) == 0 {
			return // Nothing to do
		}
		pd := i.path[len(i.path)-1]
		lastNode := pd.node
		if convert, ok := lastNode.(*BTreeInternalPage); ok {
			if pd.position == int(convert.nkey)-1 {
				// Need to move up, a level, by just pop it
				i.path = i.path[:len(i.path)-1]
				continue
			} else {
				break
			}
		} else {
			convert := lastNode.(*BTreeLeafPage)
			if pd.position == int(convert.nkv)-1 {
				// Need to move up, a level, by just pop it
				i.path = i.path[:len(i.path)-1]
				continue
			} else {
				break
			}
		}
	}
	// Start to recursively load
	i.path[len(i.path)-1].position += 1 // Update
	pd := i.path[len(i.path)-1]
	lastNode := pd.node

	// fmt.Printf("Search pd = %v %v, path len = %v\n", pd.node, pd.position, len(i.path))

	buffer := new(bytes.Buffer) // Buffer size = 0
	for {
		if convert, ok := lastNode.(*BTreeInternalPage); ok {
			buffer.Reset()
			child := convert.children[pd.position]
			// fmt.Println("Need to read at child pos = ", pd.position, ", childPtr = ", child)
			i.tree.readBlockAtPointer(child, buffer, i.file)
			// Load child
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
			// Load deeper node with first position
			new_pd := PathData{
				node:     childNode,
				position: 0,
			}
			i.path = append(i.path, new_pd)
			pd = new_pd
			lastNode = childNode
		} else {
			break
		}
	}
}

func (i *BIter) Prev() {}

func (i *BIter) Close() {
	i.file.Close()
}
