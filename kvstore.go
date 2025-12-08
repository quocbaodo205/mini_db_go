package main

type KV struct {
	fileName string
	tree     BPTreeDisk
}

func (db *KV) Open() {
	// Load or create new
	db.tree = NewBPTreeDisk(db.fileName)
}

func (db *KV) Get(key []byte) ([]byte, bool) {
	res := db.tree.Find(key)
	if res == nil {
		var valueBytes []byte = make([]byte, 0)
		return valueBytes, false
	}
	var valueBytes []byte = make([]byte, res.vallen)
	for i := 0; i < int(res.vallen); i++ {
		valueBytes[i] = res.val[i+(MAX_VAL_SIZE-int(res.vallen))]
	}
	return valueBytes, true
}

func (db *KV) Set(key []byte, val []byte) {
	db.tree.Set(key, val)
}

func (db *KV) Del(key []byte) bool {
	return db.tree.Del(key)
}
