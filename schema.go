package main

import (
	"bytes"
	"encoding/binary"
)

// [INT, STRING (Not UTF-8)], BLOB, UUID, FLOAT, ...

// Database -> Tables + internal tables (metadata, all tables definition)
// Table -> rows
// row -> columns
// column -> value

const (
	TYPE_EMPTY = 0
	TYPE_BYTES = 1 // string type
	TYPE_INT64 = 2 // int type
)

// If Type == 1: only I64 have value
// If Type == 2: only Str have value
type Value struct {
	Type uint8 // Tag union
	I64  int64
	Str  []byte
}

// Have to assume the same type
func compareValue(lhs Value, rhs Value) int {
	if lhs.Type == TYPE_INT64 {
		if lhs.I64 < rhs.I64 {
			return -1
		}
		if lhs.I64 > rhs.I64 {
			return 1
		}
	} else {
		for i := 0; i < len(lhs.Str); i++ {
			if lhs.Str[i] < rhs.Str[i] {
				return -1
			}
			if lhs.Str[i] > rhs.Str[i] {
				return 1
			}
		}
	}
	return 0
}

// Basically a row
// Cols[1] = 'date' -> Vals[1]
type Record struct {
	Cols []string
	Vals []Value
}

// Add data to a record
func (r *Record) AddStr(col string, val []byte) *Record {
	r.Cols = append(r.Cols, col)
	r.Vals = append(r.Vals, Value{
		Type: 1,
		Str:  val,
	})
	return r
}

func (r *Record) AddInt64(col string, val int64) *Record {
	r.Cols = append(r.Cols, col)
	r.Vals = append(r.Vals, Value{
		Type: 2,
		I64:  val,
	})
	return r
}

// Asume the 2 slice have the same number of elements
func compareRecord(lhs []Value, rhs []Value) int {
	for i := range lhs {
		comp := compareValue(lhs[i], rhs[i])
		if comp != 0 {
			return comp
		}
	}
	return 0
}

// Table metadata: definition: Each column is of what type
type TableDef struct {
	name    string
	Types   []uint8
	Cols    []string
	Indexes [][]string // first in dex is the primary key
	Prefix  []uint8    // auto-assigned prefixes
}

// Database schema: Internal tables
// Auto-inc: 1
// Date_create: 2025/01/01
// Store a bunch of key values
var TDEF_META = &TableDef{
	name:    "@meta",
	Types:   []uint8{TYPE_BYTES, TYPE_BYTES},
	Cols:    []string{"key", "value"},
	Indexes: [][]string{{"key", "value"}},
	Prefix:  []uint8{1},
}

// How many tables? What are the table
// What are the indexes
// Column?
// Type supported.
// ...
// Store schema inside the KV
var TDEF_TABLE = &TableDef{
	name:    "@table",
	Types:   []uint8{TYPE_BYTES, TYPE_BYTES},
	Cols:    []string{"name", "def"},
	Indexes: [][]string{{"name", "def"}},
	Prefix:  []uint8{2},
}

type DB struct {
	Path string
	kv   KV
}

func (db *DB) Open() {
	db.kv = KV{
		fileName: db.Path,
	}
	db.kv.Open()
}

// ======================= Record functions =====================

// [(name, age), date, friend_with,...]

// Check if all primary key presents and have all known columns
func checkRecord(tdef *TableDef, rec *Record) bool {
	// Check if rec have all the primary key columns
	for i := 0; i < len(rec.Cols); i++ {
		for _, idxCol := range tdef.Indexes[0] {
			if idxCol == rec.Cols[i] && rec.Vals[i].Type == 0 {
				// Found a primary key columns that have no value.
				return false
			}
		}
	}
	return true
}

// 10, ["Adam", 30] -> [10 4 A d a m 8 0 0 0 0 0 0 0 30 0]
// Contain all values in order.
func encodeKey(prefix uint8, Vals []Value) []byte {
	// Prepare a buffer for bytes writing
	var err error
	buffer := new(bytes.Buffer) // Buffer size = 0
	// First: Write the prefix
	err = binary.Write(buffer, binary.BigEndian, prefix)
	// How many vals are there
	err = binary.Write(buffer, binary.BigEndian, uint8(len(Vals)))
	for _, v := range Vals {
		// For each v, write data:
		// First, the type
		err = binary.Write(buffer, binary.BigEndian, uint8(v.Type))
		if v.Type == TYPE_INT64 {
			// Just write the int64
			err = binary.Write(buffer, binary.BigEndian, v.I64)
		} else {
			// Write the len
			err = binary.Write(buffer, binary.BigEndian, uint8(len(v.Str)))
			// Write the rest of the data.
			buffer.Write(v.Str)
		}
	}
	// No error handing here, just panic
	if err != nil {
		panic(err)
	}
	return buffer.Bytes() // Escape outside, will allocate on heap
}

func decodeVals(data []byte) []Value {
	// Make a byte buffer with the data.
	var err error
	buffer := new(bytes.Buffer) // Buffer size = 0
	buffer.Write(data)
	// First: Read the prefix. For val this should be discarded.
	var prefix uint8
	err = binary.Read(buffer, binary.BigEndian, &prefix)
	// Next, read how many values
	var n uint8
	err = binary.Read(buffer, binary.BigEndian, &n)
	res := make([]Value, 0)
	for i := 0; i < int(n); i += 1 {
		var v Value
		var vtype uint8
		err = binary.Read(buffer, binary.BigEndian, &vtype)
		if vtype == TYPE_INT64 {
			// Just read the int64
			err = binary.Read(buffer, binary.BigEndian, &v.I64)
		} else {
			// Read the len
			var l uint8
			err = binary.Read(buffer, binary.BigEndian, &l)
			var data []byte = make([]byte, l)
			buffer.Read(data)
			v.Str = data
		}
		res = append(res, v)
	}
	if err != nil {
		panic(err)
	}

	return res
}

func makeValuesWithIndex(tdef *TableDef, indexPos int, rec *Record) []Value {
	// Get record value
	recordVals := make([]Value, 0)
	for i := 0; i < len(tdef.Indexes[indexPos]); i++ {
		for j := 0; j < len(rec.Cols); j++ {
			if tdef.Indexes[indexPos][i] == rec.Cols[j] {
				recordVals = append(recordVals, rec.Vals[j])
			}
		}
	}
	return recordVals
}

// ======================= Internal DB functions ======================

// SELECT * FROM People WHERE name == 'Adam' and age == 30
// Always get from primary key: index[0] , prefix[0]
func dbGet(db *DB, tdef *TableDef, rec *Record) bool {
	// Start a transaction
	tx := KVTX{}
	tx.kv.Begin(&tx)

	// Step 1: reorder columns
	// rec{Cols[name, date, age], Val: ['Adam', nil, 30]}
	// -> rec{Cols[name, age, date], Val: ['Adam', 30, nil]}
	checkRecordRes := checkRecord(tdef, rec)
	if !checkRecordRes {
		return false
	}

	// Get record value
	recordVals := makeValuesWithIndex(tdef, 0, rec)

	// Step 2: encode the key into bytes
	key := encodeKey(tdef.Prefix[0], recordVals)

	// Step 3: query from kv store with transaction
	val, found := tx.Get(key)
	if !found {
		return false
	}

	// Step 4: decode into record
	vals := decodeVals(val)
	for i := len(tdef.Indexes[0]); i < len(tdef.Cols); i++ {
		rec.Vals[i] = vals[i-len(tdef.Indexes[0])]
	}

	// Res: rec{Cols[name, age, date], Val: ["Adam", 30, 20250101]}
	return true
}

// INSERT INTO People (name, age, date) ('bob', 31, 20252111)
func dbInsert(db *DB, tdef *TableDef, rec *Record) bool {
	// Start a transaction
	tx := KVTX{}
	tx.kv.Begin(&tx)

	// Step 1: reorder columns
	// rec{Cols[name, date, age], Val: ['Adam', nil, 30]}
	// -> rec{Cols[name, age, date], Val: ['Adam', 30, nil]}
	checkRecordRes := checkRecord(tdef, rec)
	if !checkRecordRes {
		return false
	}

	// Get record value
	recordVals := makeValuesWithIndex(tdef, 0, rec)
	// Step 2: encode the key into bytes
	key := encodeKey(tdef.Prefix[0], recordVals)

	// TODO: Step 3: Fill empty for columns not in rec

	// Step 4: encode value
	val := encodeKey(tdef.Prefix[0], rec.Vals[len(tdef.Indexes[0]):])

	// Create an update request for insert
	req := UpdateReq{
		Key:     key,
		Val:     val,
		Old:     []byte{},
		Mode:    1,
		Added:   false,
		Updated: false,
	}

	tx.Update(&req)

	return true
}

// DELETE FROM People WHERE name = "xyz" and age = 18
func dbDelete(db *DB, tdef *TableDef, rec *Record) bool {
	// Start a transaction
	tx := KVTX{}
	tx.kv.Begin(&tx)

	// Step 1: reorder columns
	// rec{Cols[name, date, age], Val: ['Adam', nil, 30]}
	// -> rec{Cols[name, age, date], Val: ['Adam', 30, nil]}
	checkRecordRes := checkRecord(tdef, rec)
	if !checkRecordRes {
		return false
	}

	// Get record value
	recordVals := makeValuesWithIndex(tdef, 0, rec)
	// Step 2: encode the key into bytes
	key := encodeKey(tdef.Prefix[0], recordVals)

	req := UpdateReq{
		Key:  key,
		Mode: 2,
	}

	// Step 3: Delete using KV store
	return tx.Update(&req)
}

func dbUpdate(db *DB, tdef *TableDef, rec *Record) bool {
	// Start a transaction
	tx := KVTX{}
	tx.kv.Begin(&tx)

	// Step 1: reorder columns
	// rec{Cols[name, date, age], Val: ['Adam', nil, 30]}
	// -> rec{Cols[name, age, date], Val: ['Adam', 30, nil]}
	checkRecordRes := checkRecord(tdef, rec)
	if !checkRecordRes {
		return false
	}

	// Get record value
	recordVals := makeValuesWithIndex(tdef, 0, rec)
	// Step 2: encode the key into bytes
	key := encodeKey(tdef.Prefix[0], recordVals)

	// Step 3: encode value
	val := encodeKey(tdef.Prefix[0], rec.Vals[len(tdef.Indexes[0]):])

	req := UpdateReq{Key: key, Val: val, Mode: 3} // Mode update
	tx.Update(&req)
	// Step 4: maintain index with update request to maintain secondary indexes
	handleUpdateRequest(db, &tx, tdef, &req)
	return req.Updated
}

// Convert from record to a table definition structure
func decodeTableDef(rec *Record) TableDef {
	// TODO:
	return TableDef{}
}

func encodeTableDef(TableDef) []uint8 {
	// TODO:
	res := make([]uint8, 0)
	return res
}

func getTableDef(db *DB, table string) *TableDef {
	// rec:{Cols = ["name"], Vals = ["People"]}
	rec := (&Record{}).AddStr("name", []byte(table))
	found := dbGet(db, TDEF_TABLE, rec)
	if !found {
		return nil
	}
	res := decodeTableDef(rec)
	return &res
}

// ========================== DB Wrapper functions ==============

// rec = {['name', 'age'], ['Adam', 30] }
// => SELCT * FROM ... WHERE name = 'Adam' and age = 30
func (db *DB) Get(table string, rec *Record) bool {
	// Step 1: Check and get table definition from table name
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false
	}

	// Step 2: Get using table definition
	return dbGet(db, tdef, rec)
}

func (db *DB) Insert(table string, rec Record) bool {
	// Step 1: Check and get table definition from table name
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false
	}

	// Step 2: Get to see if there's data
	found := db.Get(table, &rec)
	if found {
		return false // Cannot insert if same primary key
	}

	// Step 3: Insert using table definition
	return dbInsert(db, tdef, &rec)
}

// =================== TODO: Implement this =================
func (db *DB) Update(table string, rec Record) bool {
	return true
}

// =================== TODO: Implement this =================
func (db *DB) Upsert(table string, rec Record) bool {
	return true
}

func (db *DB) Delete(table string, rec Record) bool {
	// Step 1: Check and get table definition from table name
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false
	}

	// Step 2: Get to see if there's data
	found := db.Get(table, &rec)
	if !found {
		return false // Cannot insert if same primary key
	}

	// Step 3: Delete using table definition
	return dbDelete(db, tdef, &rec)
}

// Return all records
// SELECT * FROM People where c3 <= 2 AND c2 <= 1
// AND c3 >=1 AND c2 >= 2
// func (db *DB) Scan(table string, sc *Scanner) []Record {
// 	records := make([]Record, 0)
// 	// Step 1: Check and get table definition from table name
// 	tdef := getTableDef(db, table)
// 	if tdef == nil {
// 		return records
// 	}
// 	sc.tdef = tdef
// 	// TODO Init the Scanner here
// 	for {
// 		// Assume init
// 		if !sc.Valid() {
// 			break
// 		}
// 		rec := Record{}
// 		sc.Deref(&rec)
// 		records = append(records, rec)
// 		sc.Next()
// 	}
// 	return records
// }

// ========================== Maintaining indexes ==================
type UpdateReq struct {
	tree *BPTreeDisk
	// in
	Key  []byte
	Val  []byte
	Old  []byte // the value before the update
	Mode int
	// out
	Added   bool // added a new key
	Updated bool // added a new key or an old key was changed
}

// Handing update request for other indexes.
func handleUpdateRequest(db *DB, tx *KVTX, tdef *TableDef, req *UpdateReq) {
	oldVals := decodeVals(req.Old)
	newVals := decodeVals(req.Val)
	needChangeIdx := 0
	var needChangePrefix uint8 = 0
	for i := len(tdef.Indexes[0]); i < len(tdef.Cols); i++ {
		col := tdef.Cols[i]
		if compareRecord(oldVals, newVals) != 0 {
			// Check if this belong to any index
			for idx, idxCols := range tdef.Indexes[1:] {
				for _, idxCol := range idxCols {
					if idxCol == col {
						needChangeIdx = idx
						needChangePrefix = tdef.Prefix[idx]
					}
				}
			}
		}
	}
	oldRecord := Record{}
	newRecord := Record{}
	// Get record value
	oldRecordVals := make([]Value, 0)
	newRecordVals := make([]Value, 0)
	for i := 0; i < len(tdef.Indexes[needChangeIdx]); i++ {
		for j := 0; j < len(oldRecord.Cols); j++ {
			if tdef.Indexes[needChangeIdx][i] == oldRecord.Cols[j] {
				oldRecordVals = append(oldRecordVals, oldRecord.Vals[j])
				newRecordVals = append(newRecordVals, newRecord.Vals[j])
			}
		}
	}
	oldKey := encodeKey(uint8(needChangePrefix), oldRecordVals)
	newKey := encodeKey(uint8(needChangePrefix), newRecordVals)
	tx.Update(&UpdateReq{
		Key:  oldKey,
		Mode: 2,
	})
	tx.Update(&UpdateReq{
		Key:  newKey,
		Val:  req.Key,
		Mode: 1,
	})
	req.Updated = true
}

// ========================== Scanner ==============================
type Scanner struct {
	// the range, from Key1 to Key2
	Key1 Record
	Key2 Record
	// internal
	db     *DB
	tdef   *TableDef
	index  int    // which index?
	iter   *BIter // the underlying B-tree iterator
	keyEnd []byte // the encoded Key2
}

// within the range or not?
func (sc *Scanner) Valid() bool {
	if len(sc.keyEnd) == 0 {
		// Get record value
		recordVals := make([]Value, 0)
		for i := 0; i < len(sc.tdef.Indexes[sc.index]); i++ {
			for j := 0; j < len(sc.Key2.Cols); j++ {
				if sc.tdef.Indexes[sc.index][i] == sc.Key2.Cols[j] {
					recordVals = append(recordVals, sc.Key2.Vals[j])
				}
			}
		}
		sc.keyEnd = encodeKey(sc.tdef.Prefix[sc.index], recordVals)
	}
	pkeyKV := sc.iter.Deref()
	indexKeys := pkeyKV.key
	for i := range len(indexKeys) {
		if indexKeys[i] > sc.keyEnd[i] {
			// Out of range
			return false
		}
	}
	return true
}

// move the underlying B-tree iterator
func (sc *Scanner) Next() {
	// Assume init
	sc.iter.Next()
}

// fetch the current row
// func (sc *Scanner) Deref(rec *Record) {
// 	pkeyKV := sc.iter.Deref()
// 	pkeyData := pkeyKV.val
// 	pkeyVal, _ := sc.db.kv.Get(pkeyData[:])

// 	// Decode primary keys to columns.
// 	pkeyVals := decodeVals(pkeyData[:])
// 	recordVals := decodeVals(pkeyVal)
// 	for i := 0; i < len(sc.tdef.Indexes[0]); i++ {
// 		rec.Vals[i] = pkeyVals[i]
// 	}
// 	for i := len(sc.tdef.Indexes[0]); i < len(sc.tdef.Cols); i++ {
// 		rec.Vals[i] = recordVals[i-len(sc.tdef.Indexes[0])]
// 	}
// }

// ============================= Transaction ======================

// History and conflict detection
type StoreKey struct {
	key []byte
}

// Design for transaction in transaction
type CommittedTX struct {
	mt      MetaPage
	version uint64
	writes  []StoreKey
}

type KVTX struct {
	kv      *KV
	version uint64

	// Concurrency control
	snapshot MetaPage
	pending  *MetaPage // Can be null

	// Current read and written rows
	reads  []StoreKey
	writes []StoreKey
}

// begin a transaction: Store snapshot
func (kv *KV) Begin(tx *KVTX) {
	tx.kv = kv
	// TODO: Generate a new version
	tx.version = 100
	tx.snapshot = tx.kv.LoadMetaPage()
}

// end a transaction: commit updates; rollback on error
func (kv *KV) Commit(tx *KVTX) bool {
	mt, _ := tx.GetMeta()
	if detectConflicts(kv, tx) {
		return false
	}
	kv.history = append(kv.history, CommittedTX{
		version: tx.version,
		writes:  tx.writes,
		mt:      mt,
	})
	// Do not write to disk yet, wait for writter
	return true
}

// end a transaction: rollback
// Remove all pending operations
func (kv *KV) Abort(tx *KVTX) {
}

// point query. combines captured updates with the snapshot
// true: Get return the snapshot
// false: Get return a pending
func (tx *KVTX) GetMeta() (MetaPage, bool) {
	if tx.pending != nil {
		val := *tx.pending
		return val, false
	} else {
		return tx.snapshot, true
	}
}

func (tx *KVTX) Get(key []byte) ([]byte, bool) {
	mt, _ := tx.GetMeta()
	val, exist := tx.kv.Get(mt, key)
	if exist {
		tx.reads = append(tx.reads, StoreKey{
			key: key,
		})
	}
	return val, exist
}

func (tx *KVTX) Update(req *UpdateReq) bool {
	mt, _ := tx.GetMeta()
	if req.Mode == 1 { // Insert
		*tx.pending = tx.kv.Set(mt, req.Key, req.Val)
	} else if req.Mode == 2 { // Del
		ok, newMetaPage := tx.kv.Del(mt, req.Key)
		if ok {
			*tx.pending = newMetaPage
		}
	} else { // Update
		*tx.pending = tx.kv.Set(mt, req.Key, req.Val)
	}
	// After update, put a history write.
	tx.writes = append(tx.writes, StoreKey{
		req.Key,
	})
	return true
}

func rangesOverlap(reads []StoreKey, writes []StoreKey) bool {
	for _, readKey := range reads {
		for _, writeKey := range writes {
			isEql := true
			for idx, rk := range readKey.key {
				if writeKey.key[idx] != rk {
					isEql = false
					break
				}
			}
			if isEql {
				return true
			}
		}
	}
	return false
}

func detectConflicts(kv *KV, tx *KVTX) bool {
	// First the last transaction that is of smaller version in the history
	for i := len(kv.history) - 1; i >= 0; i-- {
		if tx.version >= kv.history[i].version {
			continue
		}
		if rangesOverlap(tx.reads, kv.history[i].writes) {
			return true
		}
	}
	return false
}
