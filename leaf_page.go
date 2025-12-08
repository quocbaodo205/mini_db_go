package main

import (
	"bytes"
	"encoding/binary"
)

// =========================================================================

const MAX_VAL_SIZE = 8

// 3: [1, 7, 255]
// [0 0 0 0 0 0 1 7 255]
type KeyVal struct {
	keylen uint16
	vallen uint16
	key    [MAX_KEY_SIZE]uint8 // Big endian storage
	val    [MAX_VAL_SIZE]uint8 // Big endian storage
}

func NewKeyValFromInt(inputKey int64, inputVal int64) KeyVal {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, inputKey)
	data_slice := buf.Bytes()
	key_len := len(data_slice)
	var key [MAX_KEY_SIZE]uint8
	for i := MAX_KEY_SIZE - key_len; i < MAX_KEY_SIZE; i += 1 {
		key[i] = data_slice[i-(MAX_KEY_SIZE-key_len)] // data[10] = slice[0] | data[11] = slice[1] ...
	}
	buf = new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, inputKey)
	data_slice = buf.Bytes()
	val_len := len(data_slice)
	var val [MAX_VAL_SIZE]uint8
	for i := MAX_VAL_SIZE - val_len; i < MAX_VAL_SIZE; i += 1 {
		key[i] = data_slice[i-(MAX_VAL_SIZE-val_len)] // data[10] = slice[0] | data[11] = slice[1] ...
	}
	return KeyVal{
		keylen: uint16(key_len),
		vallen: uint16(val_len),
		key:    key,
		val:    val,
	}
}

func NewKeyValFromBytes(inputKey []byte, inputVal []byte) KeyVal {
	key_len := len(inputKey)
	var key [MAX_KEY_SIZE]uint8
	for i := MAX_KEY_SIZE - key_len; i < MAX_KEY_SIZE; i += 1 {
		key[i] = inputKey[i-(MAX_KEY_SIZE-key_len)] // data[10] = slice[0] | data[11] = slice[1] ...
	}
	val_len := len(inputVal)
	var val [MAX_VAL_SIZE]uint8
	for i := MAX_VAL_SIZE - val_len; i < MAX_VAL_SIZE; i += 1 {
		key[i] = inputVal[i-(MAX_VAL_SIZE-val_len)] // data[10] = slice[0] | data[11] = slice[1] ...
	}
	return KeyVal{
		keylen: uint16(key_len),
		vallen: uint16(val_len),
		key:    key,
		val:    val,
	}
}

func (k *KeyVal) write_to_buffer(buffer *bytes.Buffer) {
	var err error
	err = binary.Write(buffer, binary.BigEndian, k.keylen)
	err = binary.Write(buffer, binary.BigEndian, k.vallen)
	for i := MAX_KEY_SIZE - k.keylen; i < MAX_KEY_SIZE; i += 1 {
		err = binary.Write(buffer, binary.BigEndian, k.key[i])
	}
	for i := MAX_VAL_SIZE - k.vallen; i < MAX_VAL_SIZE; i += 1 {
		err = binary.Write(buffer, binary.BigEndian, k.val[i])
	}
	if err != nil {
		panic(err)
	}
}

func (k *KeyVal) read_from_buffer(buffer *bytes.Buffer) {
	var err error
	err = binary.Read(buffer, binary.BigEndian, &k.keylen)
	err = binary.Read(buffer, binary.BigEndian, &k.vallen)
	for i := MAX_KEY_SIZE - k.keylen; i < MAX_KEY_SIZE; i += 1 {
		err = binary.Read(buffer, binary.BigEndian, &k.key[i])
	}
	for i := MAX_VAL_SIZE - k.vallen; i < MAX_VAL_SIZE; i += 1 {
		err = binary.Read(buffer, binary.BigEndian, &k.val[i])
	}
	if err != nil {
		panic(err)
	}
}

func (k *KeyVal) compare(rhs *KeyVal) int {
	res := 0
	for i := 0; i < MAX_KEY_SIZE; i += 1 {
		if k.key[i] < rhs.key[i] {
			return -1
		}
		if k.key[i] > rhs.key[i] {
			return 1
		}
	}
	return res
}

// =========================================================================

// Define leaf node
type BTreeLeafPage struct {
	header PageHeader
	nkv    int
	kv     [LEAF_MAX_KV]KeyVal
}

func NewLPage() BTreeLeafPage {
	var new_kv [LEAF_MAX_KV]KeyVal
	return BTreeLeafPage{
		header: PageHeader{
			page_type:         2,
			next_page_pointer: 0,
		},
		nkv: 0,
		kv:  new_kv,
	}
}

func (p *BTreeLeafPage) write_to_buffer(buffer *bytes.Buffer) {
	var err error
	p.header.write_to_buffer(buffer)
	err = binary.Write(buffer, binary.BigEndian, p.nkv)
	for i := 0; i < int(p.nkv); i += 1 {
		p.kv[i].write_to_buffer(buffer)
	}
	if err != nil {
		panic(err)
	}
}

func (p *BTreeLeafPage) read_from_buffer(buffer *bytes.Buffer, isReadHeader bool) {
	var err error
	if isReadHeader {
		p.header.read_from_buffer(buffer)
	}
	err = binary.Read(buffer, binary.BigEndian, p.nkv)
	for i := 0; i < int(p.nkv); i += 1 {
		p.kv[i].read_from_buffer(buffer)
	}
	if err != nil {
		panic(err)
	}
}

// Find last position so that the key <= find_key
func (node *BTreeLeafPage) FindLastLE(findKV *KeyVal) int {
	pos := -1
	for i := 0; i < int(node.nkv); i++ {
		if node.kv[i].compare(findKV) <= 0 {
			pos = i
		}
	}
	return pos
}

// Insert a key-children pair into the Leaf Node
func (node *BTreeLeafPage) InsertKV(insertKV *KeyVal) {
	// Find last less or equal as position to insert
	pos := node.FindLastLE(insertKV)
	// [ 1,4,7,| | ] -> insert 3
	// [ 1,| |,4,7 ] -> insert 3
	for i := node.nkv - 1; i > pos; i-- {
		node.kv[i+1] = node.kv[i]
	}
	node.kv[pos+1] = *insertKV
	// [ 1,3,4,7 ]
	node.nkv += 1
}

// Delete a key val from Leaf Node
// Assume always able to find exact
func (node *BTreeLeafPage) DelKV(delKV *KeyVal) {
	// Find last less or equal as position to delete
	pos := node.FindLastLE(delKV)
	for i := pos; i < int(node.nkv)-1; i++ {
		node.kv[i] = node.kv[i+1]
	}
	node.nkv -= 1
	node.kv[int(node.nkv)] = KeyVal{}
}

// Split a node into 2 equal part
func (node *BTreeLeafPage) Split() BTreeLeafPage {
	var newKV [LEAF_MAX_KV]KeyVal
	// Split in the middle
	pos := node.nkv / 2
	// [ 1 , 2 , 0 , 0 ] -> pos = 2
	// [ 3 , 4 , 0 , 0 ]
	for i := pos; i < node.nkv; i++ {
		newKV[i-pos] = node.kv[i] // n[0] = o[2]
		node.kv[i] = KeyVal{}
	}
	newNode := BTreeLeafPage{
		nkv: node.nkv - pos,
		kv:  newKV,
	}
	node.nkv = pos
	return newNode
}
