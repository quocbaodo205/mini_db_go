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

// Basically a row
// Cols[1] = 'date' -> Vals[1]
type Record struct {
	Cols []string
	Vals []Value
}

// Basically a row
// Cols[1] = 'date' -> Vals[1]
type RangeRecord struct {
	Cols      []string
	ValStarts []Value
	ValEnds   []Value
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

// Table metadata: definition: Each column is of what type
// Also tell which columns is primary key
// [(name, age), date, friend_with,...]
type TableDef struct {
	name   string
	Types  []uint8
	Cols   []string
	PKeys  uint8 // PKeys = 2: first 2 columns is the primary key
	Prefix uint8
}

// Database schema: Internal tables
// Auto-inc: 1
// Date_create: 2025/01/01
// Store a bunch of key values
var TDEF_META = &TableDef{
	name:   "@meta",
	Types:  []uint8{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "value"},
	PKeys:  2,
	Prefix: 1,
}

// How many tables? What are the table
// What are the indexes
// Column?
// Type supported.
// ...
// Store schema inside the KV
var TDEF_TABLE = &TableDef{
	name:   "@table",
	Types:  []uint8{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"name", "def"},
	PKeys:  2,
	Prefix: 2,
}

type DB struct {
	Path string
	kv   KV
}

// ======================= Table functions ======================

// ======================= Record functions =====================

// [(name, age), date, friend_with,...]

// Check if all primary key presents and have all known columns
// rec{Cols[name, date, age], Val: ['Adam', nil, 30]}
// -> rec{Cols[name, age, date], Val: ['Adam', 30, nil]}
func checkRecord(tdef *TableDef, rec *Record) bool {
	// TODO: Reorder and check here
	if 1 == 1 {
		return false
	}
	return true
}

// Check if all primary key presents and have all known columns
// rec{Cols[name, date, age], Val: ['Adam', nil, 30]}
// -> rec{Cols[name, age, date], Val: ['Adam', 30, nil]}
func checkRangeRecord(tdef *TableDef, rec *RangeRecord) bool {
	// TODO: Reorder and check here
	if 1 == 1 {
		return false
	}
	return true
}

// 10, ["Adam", 30] -> [10 4 A d a m 8 0 0 0 0 0 0 0 30 0]
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

// SELECT * FROM People WHERE name == 'Adam' and age == 30
func dbGet(db *DB, tdef *TableDef, rec *Record) bool {
	// Step 1: reorder columns
	// rec{Cols[name, date, age], Val: ['Adam', nil, 30]}
	// -> rec{Cols[name, age, date], Val: ['Adam', 30, nil]}
	checkRecordRes := checkRecord(tdef, rec)
	if !checkRecordRes {
		return false
	}

	// Step 2: encode the key into bytes
	key := encodeKey(tdef.Prefix, rec.Vals[:tdef.PKeys])

	// Step 3: query from kv store
	val, found := db.kv.Get(key)
	if !found {
		return false
	}

	// Step 4: decode into record
	vals := decodeVals(val)
	for i := tdef.PKeys; i < uint8(len(tdef.Cols)); i++ {
		rec.Vals[i] = vals[i-tdef.PKeys]
	}

	// Res: rec{Cols[name, age, date], Val: ["Adam", 30, 20250101]}
	return true
}

// SELECT * FROM People WHERE age >= 10 and age <= 30
// -> Find: key = [.... (decode age = 10)] -> iter next
func dbRangeGet(db *DB, tdef *TableDef, rRec *RangeRecord) ([]Record, bool) {
	var allRecords = make([]Record, 0)
	// Step 1: reorder columns
	// rec{Cols[name, date, age], Val: ['Adam', nil, 30]}
	// -> rec{Cols[name, age, date], Val: ['Adam', 30, nil]}
	checkRecordRes := checkRangeRecord(tdef, rRec)
	if !checkRecordRes {
		return allRecords, false
	}

	// Step 2: encode the keyStart into bytes
	keyStart := encodeKey(tdef.Prefix, rRec.ValStarts[:tdef.PKeys])
	keyEnd := encodeKey(tdef.Prefix, rRec.ValEnds[:tdef.PKeys])

	// Step 3: query from kv store
	vals, found := db.kv.GetRange(keyStart, keyEnd)
	if !found {
		return allRecords, false
	}

	// Step 4: decode into record
	for _, v := range vals {
		val := decodeVals(v)
		allRecords = append(allRecords, Record{
			Cols: rRec.Cols,
			Vals: val,
		})
	}

	// Res: rec{Cols[name, age, date], Val: ["Adam", 30, 20250101]}
	return allRecords, true
}

// INSERT INTO People (name, age, date) ('bob', 31, 20252111)
func dbInsert(db *DB, tdef *TableDef, rec *Record) bool {
	// Step 1: reorder columns
	// rec{Cols[name, date, age], Val: ['Adam', nil, 30]}
	// -> rec{Cols[name, age, date], Val: ['Adam', 30, nil]}
	checkRecordRes := checkRecord(tdef, rec)
	if !checkRecordRes {
		return false
	}

	// Step 2: encode the key into bytes
	key := encodeKey(tdef.Prefix, rec.Vals[:tdef.PKeys])

	// TODO: Step 3: Fill empty for columns not in rec

	// Step 4: encode value
	val := encodeKey(tdef.Prefix, rec.Vals[tdef.PKeys:])

	db.kv.Set(key, val)
	return true
}

// DELETE FROM People WHERE name = "xyz" and age = 18
func dbDelete(db *DB, tdef *TableDef, rec *Record) bool {
	// Step 1: reorder columns
	// rec{Cols[name, date, age], Val: ['Adam', nil, 30]}
	// -> rec{Cols[name, age, date], Val: ['Adam', 30, nil]}
	checkRecordRes := checkRecord(tdef, rec)
	if !checkRecordRes {
		return false
	}

	// Step 2: encode the key into bytes
	key := encodeKey(tdef.Prefix, rec.Vals[:tdef.PKeys])

	// Step 3: Delete using KV store
	return db.kv.Del(key)
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

// rec = {['age'], [(10, 30)] }
// => SELCT * FROM ... WHERE age >= 30 and age <= 60
func (db *DB) RangeQuery(table string, rec *RangeRecord) ([]Record, bool) {
	var allRecords = make([]Record, 0)
	// Step 1: Check and get table definition from table name
	tdef := getTableDef(db, table)
	if tdef == nil {
		return allRecords, false
	}

	// Step 2: Get using table definition
	return dbRangeGet(db, tdef, rec)
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
