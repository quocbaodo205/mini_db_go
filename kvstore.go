package main

type KV struct {
	fileName string
	tree     BPTreeDisk
}

func (kv *KV) Open() {
	// Load or create new
	kv.tree = NewBPTreeDisk(kv.fileName)
}

func (kv *KV) Get(key []byte) ([]byte, bool) {
	res := kv.tree.Find(key)
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

func (kv *KV) Set(key []byte, val []byte) {
	kv.tree.Set(key, val)
}

func (kv *KV) Del(key []byte) bool {
	return kv.tree.Del(key)
}
