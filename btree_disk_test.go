package main

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
	"time"
)

func intToSlice(x int64) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, x)
	return buf.Bytes()
}

func TestBTreeDisk(t *testing.T) {
	maxNum := 100
	// Create a new BTreeDisk using a test file
	test_db := NewBPTreeDisk("test_db.db")
	// Insert test: insert 10 nodes from 1->10 to check if it's good.
	for i := 1; i <= maxNum; i++ {
		// d := intToSlice(int64(i))
		// fmt.Printf("insert key = %v\n", d)
		test_db.Insert(intToSlice(int64(i)), intToSlice(int64(i)))
		// fmt.Println("=========================================")
	}
	// Find test: Find these kv if they are the same.
	for i := 1; i <= maxNum; i++ {
		kv := test_db.Find(intToSlice(int64(i)))
		expected := NewKeyValFromInt(int64(i), int64(i))
		if kv == nil {
			t.Errorf("Find test failed: Cannot find key = %d", i)
			return
		}
		if *kv != expected {
			t.Errorf("Find test failed: val not expected. Expected = %v, got %v", expected, *kv)
		}
	}
	// Set test: Set to i + 5
	for i := 1; i <= maxNum; i++ {
		// d := intToSlice(int64(i))
		// fmt.Printf("set key = %v\n", d)
		test_db.Set(intToSlice(int64(i)), intToSlice(int64(i+5)))
		// fmt.Println("=========================================")
	}
	// Find test: Find these kv if they are the same.
	for i := 1; i <= maxNum; i++ {
		kv := test_db.Find(intToSlice(int64(i)))
		expected := NewKeyValFromInt(int64(i), int64(i+5))
		if kv == nil {
			t.Errorf("Find test failed: Cannot find key = %d", i)
			return
		}
		if *kv != expected {
			t.Errorf("Find test failed: val not expected. Expected = %v, got %v", expected, *kv)
		}
	}
	// Iter test: Get an iterator and next 10 times. Should have the correct kv
	for i := 1; i <= maxNum-10; i++ {
		// kv := test_db.Find(intToSlice(int64(i)))
		iter := test_db.SeekGE(intToSlice(int64(i)))
		for j := range 10 {
			kv := iter.Deref()
			expected := NewKeyValFromInt(int64(i+j), int64(i+j+5))
			if kv != expected {
				t.Errorf("Iter test failed for i = %d and j = %d: val not expected. Expected = %v, got %v", i, j, expected, kv)
				return
			}

			iter.Next()
		}
	}
	// Del test: del odd of them
	for i := 1; i <= maxNum; i++ {
		if i%2 == 0 {
			continue
		}
		// d := intToSlice(int64(i))
		// fmt.Printf("del key = %v\n", d)
		test_db.Del(intToSlice(int64(i)))
		// fmt.Println("=========================================")
	}
	// Find test: Find these kv if they are the same.
	for i := 1; i <= maxNum; i++ {
		kv := test_db.Find(intToSlice(int64(i)))
		if i%2 == 0 {
			expected := NewKeyValFromInt(int64(i), int64(i+5))
			if kv == nil {
				t.Errorf("Find test failed: Cannot find key = %d", i)
				return
			}
			if *kv != expected {
				t.Errorf("Find test failed: val not expected. Expected = %v, got %v", expected, *kv)
			}
		} else {
			if kv != nil {
				t.Errorf("Find test failed: Expected key to be nil, found = %v", *kv)
				return
			}
		}
	}
}

func TestBTreeDisk_Shuffle(t *testing.T) {
	maxNum := 2000
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	numbers := make([]int, maxNum)
	for i := range maxNum {
		numbers[i] = i + 1
	}

	r.Shuffle(maxNum, func(i, j int) {
		numbers[i], numbers[j] = numbers[j], numbers[i]
	})
	// Create a new BTreeDisk using a test file
	test_db := NewBPTreeDisk("test_db.db")
	// Insert test: insert to check if it's good.
	for _, i := range numbers {
		// d := intToSlice(int64(i))
		// fmt.Printf("insert key = %v\n", d)
		test_db.Insert(intToSlice(int64(i)), intToSlice(int64(i)))
		// fmt.Println("=========================================")
	}
	// Find test: Find these kv if they are the same.
	r.Shuffle(maxNum, func(i, j int) {
		numbers[i], numbers[j] = numbers[j], numbers[i]
	})
	for _, i := range numbers {
		kv := test_db.Find(intToSlice(int64(i)))
		expected := NewKeyValFromInt(int64(i), int64(i))
		if kv == nil {
			t.Errorf("Find test failed: Cannot find key = %d", i)
			return
		}
		if *kv != expected {
			t.Errorf("Find test failed: val not expected. Expected = %v, got %v", expected, *kv)
		}
	}
	// Set test: Set to i + 5
	r.Shuffle(maxNum, func(i, j int) {
		numbers[i], numbers[j] = numbers[j], numbers[i]
	})
	for _, i := range numbers {
		// d := intToSlice(int64(i))
		// fmt.Printf("set key = %v\n", d)
		test_db.Set(intToSlice(int64(i)), intToSlice(int64(i+5)))
		// fmt.Println("=========================================")
	}
	// Find test: Find these kv if they are the same.
	r.Shuffle(maxNum, func(i, j int) {
		numbers[i], numbers[j] = numbers[j], numbers[i]
	})
	for _, i := range numbers {
		kv := test_db.Find(intToSlice(int64(i)))
		expected := NewKeyValFromInt(int64(i), int64(i+5))
		if kv == nil {
			t.Errorf("Find test failed: Cannot find key = %d", i)
			return
		}
		if *kv != expected {
			t.Errorf("Find test failed: val not expected. Expected = %v, got %v", expected, *kv)
		}
	}
	// Del test: del odd of them
	r.Shuffle(maxNum, func(i, j int) {
		numbers[i], numbers[j] = numbers[j], numbers[i]
	})
	for _, i := range numbers {
		if i%2 == 0 {
			continue
		}
		// d := intToSlice(int64(i))
		// fmt.Printf("del key = %v\n", d)
		test_db.Del(intToSlice(int64(i)))
		// fmt.Println("=========================================")
	}
	// Find test: Find these kv if they are the same.
	r.Shuffle(maxNum, func(i, j int) {
		numbers[i], numbers[j] = numbers[j], numbers[i]
	})
	for _, i := range numbers {
		kv := test_db.Find(intToSlice(int64(i)))
		if i%2 == 0 {
			expected := NewKeyValFromInt(int64(i), int64(i+5))
			if kv == nil {
				t.Errorf("Find test failed: Cannot find key = %d", i)
				return
			}
			if *kv != expected {
				t.Errorf("Find test failed: val not expected. Expected = %v, got %v", expected, *kv)
			}
		} else {
			if kv != nil {
				t.Errorf("Find test failed: Expected key to be nil, found = %v", *kv)
				return
			}
		}
	}
}
