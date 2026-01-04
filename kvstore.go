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

func (kv *KV) GetRange(keyStart []byte, keyEnd []byte) ([][]byte, bool) {
	res := make([][]byte, 0)
	iter := kv.tree.SeekGE(keyStart)
	if iter == nil {
		return res, false
	}
	for {
		kv := iter.Deref()
		// Compare 2 keys
		var bigger = false
		for i := range len(kv.key) {
			if kv.key[i] > keyEnd[i] {
				bigger = true
				break
			}
		}
		if bigger {
			break
		}
		var valueBytes []byte = make([]byte, len(res))
		for i := 0; i < int(kv.vallen); i++ {
			valueBytes[i] = kv.val[i+(MAX_VAL_SIZE-int(kv.vallen))]
		}
		res = append(res, valueBytes)
	}
	return res, true
}

func (kv *KV) Set(key []byte, val []byte) {
	kv.tree.Set(key, val)
}

func (kv *KV) Del(key []byte) bool {
	return kv.tree.Del(key)
}
