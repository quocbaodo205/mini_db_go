package main

import (
	// "bytes"
	// "encoding/binary"
	// "fmt"
	// "math/rand"
	"testing"
	// "time"
)

func TestSchema_Record(t *testing.T) {
	// Test check record
	studentTable := TableDef{
		name:  "Student",
		Types: []uint8{TYPE_INT64, TYPE_INT64, TYPE_BYTES, TYPE_INT64},
		Cols:  []string{"id", "age", "name", "join_time"},
		Indexes: [][]string{
			{"id"},
			{"age", "name"},
		},
		Prefix: []uint8{11, 12},
	}
	recordWithID := Record{
		Cols: []string{"id", "age", "name"},
		Vals: []Value{
			{
				Type: TYPE_INT64,
				I64:  1,
			},
			{
				Type: TYPE_EMPTY,
			},
			{
				Type: TYPE_EMPTY,
			},
		},
	}
	if !checkRecord(&studentTable, &recordWithID) {
		t.Errorf("Not all pkey column found!")
	}
	recordWithNoID := Record{
		Cols: []string{"id", "age", "name"},
		Vals: []Value{
			{
				Type: TYPE_EMPTY,
			},
			{
				Type: TYPE_INT64,
				I64:  32,
			},
			{
				Type: TYPE_EMPTY,
			},
		},
	}
	if checkRecord(&studentTable, &recordWithNoID) {
		t.Errorf("Expected not found all pkey, but found!")
	}
	original := []Value{
		{
			Type: TYPE_INT64,
			I64:  32,
		},
		{
			Type: TYPE_BYTES,
			Str:  []byte("adam"),
		},
	}
	// Encode / decode test
	encoded := encodeKey(11, original)
	decoded := decodeVals(encoded)
	if len(original) != len(decoded) {
		t.Fatalf("Encode / Decode length is different, expected = %v, actual = %v", len(original), len(decoded))
	}
	for i := range decoded {
		if compareValue(original[i], decoded[i]) != 0 {
			t.Errorf("Value different at %v, expected = %v, actual = %v", i, original[i], decoded[i])
		}
	}
	// Index values
	recWithAll := Record{}
	recWithAll.AddInt64("id", 1)
	recWithAll.AddInt64("age", 30)
	recWithAll.AddStr("name", []byte("bao"))
	recWithAll.AddInt64("join_time", 2025)
	indexVals := makeValuesWithIndex(&studentTable, 1, &recWithAll)
	// Check if contains all needed values
	if len(indexVals) != 2 {
		t.Fatalf("Index Vals len is different, expected = %v, actual = %v", 2, len(indexVals))
	}
	expect := []Value{
		{
			Type: TYPE_INT64,
			I64:  30,
		},
		{
			Type: TYPE_BYTES,
			Str:  []byte("bao"),
		},
	}
	for i := range indexVals {
		if compareValue(expect[i], indexVals[i]) != 0 {
			t.Errorf("Value different at %v, expected = %v, actual = %v", i, expect[i], indexVals[i])
		}
	}
}
