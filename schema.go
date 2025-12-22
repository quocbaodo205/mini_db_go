package main

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

// 10, ["Adam", 30] -> [10 4 A d a m 8 0 0 0 0 0 0 0 30 0]
func encodeKey(prefix uint8, Vals []Value) []byte {
	// TODO: Encoding it into []byte
	res := make([]uint8, 0)
	return res
}

func decodeVals(data []byte) []Value {
	// TODO
	res := make([]Value, 0)
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
func (db *DB) Update(table string, rec Record) bool

// =================== TODO: Implement this =================
func (db *DB) Upsert(table string, rec Record) bool

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
