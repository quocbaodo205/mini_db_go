package main

import (
	"bytes"
	"encoding/binary"
)

// 0: Meta Page
// 1: Internal Page
// 2: Leaf Page
// ...: not support
type PageHeader struct {
	page_type         uint8
	next_page_pointer uint64
}

func (h *PageHeader) write_to_buffer(buffer *bytes.Buffer) {
	// int 183746238746
	// big endian:    [0 0 0 0 0 0 0 ... 255 255 255 1 2 3 4 5]
	// little endian: [5 4 3 2 1 255 255 255 ... 0 0 0 0 0 0 0]
	var err error
	err = binary.Write(buffer, binary.BigEndian, h.page_type)
	err = binary.Write(buffer, binary.BigEndian, h.next_page_pointer)
	if err != nil {
		panic(err)
	}
	// {page_type = 1, next = 1024} -> [ 1 0 0 0 0 0 0 255 255 ]
}

func (h *PageHeader) read_from_buffer(buffer *bytes.Buffer) {
	var err error
	binary.Read(buffer, binary.BigEndian, &h.page_type)
	binary.Read(buffer, binary.BigEndian, &h.next_page_pointer)
	if err != nil {
		panic(err)
	}
	// {page_type = 1, next = 1024}, buffer = [ 1 0 0 0 0 0 0 255 255 ]
}

// =========================================================================

type MetaPage struct {
	header PageHeader
}

func (p *MetaPage) write_to_buffer(buffer *bytes.Buffer) {
	p.header.write_to_buffer(buffer)
}

func (p *MetaPage) read_from_buffer(buffer *bytes.Buffer) {
	p.header.read_from_buffer(buffer)
}

// =========================================================================

const MAX_KEY_SIZE = 8

// 3: [1, 7, 255]
// [0 0 0 0 0 0 1 7 255]
type KeyEntry struct {
	len  uint16
	data [MAX_KEY_SIZE]uint8 // Big endian storage
}

func NewKeyEntryFromInt(input int64) KeyEntry {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, input)
	data_slice := buf.Bytes()
	data_len := len(data_slice)
	var data [MAX_KEY_SIZE]uint8
	for i := MAX_KEY_SIZE - data_len; i < MAX_KEY_SIZE; i += 1 {
		data[i] = data_slice[i-(MAX_KEY_SIZE-data_len)] // data[10] = slice[0] | data[11] = slice[1] ...
	}
	return KeyEntry{
		len:  uint16(data_len),
		data: data,
	}
}

// our key: [0 0 0 0 255 255 1 2]
// input  : [0 0 0 0 0 0 1 2]
func NewKeyEntryFromBytes(input []byte) KeyEntry {
	data_len := len(input)
	var data [MAX_KEY_SIZE]uint8
	for i := MAX_KEY_SIZE - data_len; i < MAX_KEY_SIZE; i += 1 {
		data[i] = input[i-(MAX_KEY_SIZE-data_len)] // data[10] = slice[0] | data[11] = slice[1] ...
	}
	return KeyEntry{
		len:  uint16(data_len),
		data: data,
	}
}

func (k *KeyEntry) write_to_buffer(buffer *bytes.Buffer) {
	var err error
	err = binary.Write(buffer, binary.BigEndian, k.len)
	for i := MAX_KEY_SIZE - k.len; i < MAX_KEY_SIZE; i += 1 {
		err = binary.Write(buffer, binary.BigEndian, k.data[i])
	}
	if err != nil {
		panic(err)
	}
}

func (k *KeyEntry) read_from_buffer(buffer *bytes.Buffer) {
	var err error
	err = binary.Read(buffer, binary.BigEndian, &k.len)
	for i := MAX_KEY_SIZE - k.len; i < MAX_KEY_SIZE; i += 1 {
		err = binary.Read(buffer, binary.BigEndian, &k.data[i])
	}
	if err != nil {
		panic(err)
	}
}

func (k *KeyEntry) compare(rhs *KeyEntry) int {
	res := 0
	for i := 0; i < MAX_KEY_SIZE; i += 1 {
		if k.data[i] < rhs.data[i] {
			return -1
		}
		if k.data[i] > rhs.data[i] {
			return 1
		}
	}
	return res
}

// =========================================================================

// [header | u8 u8 | k0 k1 k2 ... | 0 0 0 0 0 0 ... ]
type BTreeInternalPage struct {
	header   PageHeader
	nkey     uint16
	keys     [INTERNAL_MAX_KEY]KeyEntry
	children [INTERNAL_MAX_KEY]uint64
}

func (p *BTreeInternalPage) write_to_buffer(buffer *bytes.Buffer) {
	var err error
	p.header.write_to_buffer(buffer)
	err = binary.Write(buffer, binary.BigEndian, p.nkey)
	for i := 0; i < int(p.nkey); i += 1 {
		p.keys[i].write_to_buffer(buffer)
	}
	for i := 0; i < int(p.nkey); i += 1 {
		err = binary.Write(buffer, binary.BigEndian, p.children[i])
	}
	if err != nil {
		panic(err)
	}
}

func (p *BTreeInternalPage) read_from_buffer(buffer *bytes.Buffer, isReadHeader bool) {
	var err error
	if isReadHeader {
		p.header.read_from_buffer(buffer)
	}
	err = binary.Read(buffer, binary.BigEndian, &p.nkey)
	for i := 0; i < int(p.nkey); i += 1 {
		p.keys[i].read_from_buffer(buffer)
	}
	for i := 0; i < int(p.nkey); i += 1 {
		err = binary.Read(buffer, binary.BigEndian, &p.children[i])
	}
	if err != nil {
		panic(err)
	}
}

func NewIPage() BTreeInternalPage {
	var new_keys [INTERNAL_MAX_KEY]KeyEntry
	var new_children [INTERNAL_MAX_KEY]uint64
	return BTreeInternalPage{
		nkey:     0,
		keys:     new_keys,
		children: new_children,
		header: PageHeader{
			page_type:         1,
			next_page_pointer: 0,
		},
	}
}

// Find last position so that the key <= find_key
func (node *BTreeInternalPage) FindLastLE(findKey *KeyEntry) int {
	pos := -1
	for i := 0; i < int(node.nkey); i++ {
		if node.keys[i].compare(findKey) <= 0 {
			pos = i
		}
	}
	return pos
}

// Insert a key-children pair into the Internal Node
func (node *BTreeInternalPage) InsertKV(insertKey *KeyEntry, insertChildPPtr uint64) {
	// Find last less or equal as position to insert
	pos := node.FindLastLE(insertKey)
	for i := int(node.nkey) - 1; i > pos; i-- {
		node.keys[i+1] = node.keys[i]
		node.children[i+1] = node.children[i]
	}
	node.keys[pos+1] = *insertKey
	node.children[pos+1] = insertChildPPtr
	node.nkey += 1
}

func (node *BTreeInternalPage) DelKVAtPos(pos int) {
	for i := pos + 1; i < int(node.nkey)-1; i++ {
		node.keys[i] = node.keys[i+1]
		node.children[i] = node.children[i+1]
	}
	node.nkey -= 1
	node.keys[int(node.nkey)] = KeyEntry{}
	node.children[int(node.nkey)] = 0

}

// Split a node into 2 equal part
func (node *BTreeInternalPage) Split() BTreeInternalPage {
	var newKeys [INTERNAL_MAX_KEY]KeyEntry
	var newChildren [INTERNAL_MAX_KEY]uint64
	// Split in the middle
	pos := node.nkey / 2
	// [ 1 , 2 , 0 , 0 ] -> pos = 2
	// [ 3 , 4 , 0 , 0 ]
	for i := pos; i < node.nkey; i++ {
		newKeys[i-pos] = node.keys[i] // n[0] = o[2]
		newChildren[i-pos] = node.children[i]
		node.keys[i] = KeyEntry{}
		node.children[i] = 0
	}
	newNode := BTreeInternalPage{
		nkey:     node.nkey - pos,
		keys:     newKeys,
		children: newChildren,
	}
	node.nkey = pos
	return newNode
}
