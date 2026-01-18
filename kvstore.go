package main

type KV struct {
	fileName string
	tree     BPTreeDisk
	history  []CommittedTX
}

func (kv *KV) Open() {
	// Load or create new
	kv.tree = NewBPTreeDisk(kv.fileName)
}

func (kv *KV) LoadMetaPage() MetaPage {
	return kv.tree.LoadMetaPage()
}

func (kv *KV) WriteMetaPage(metaPage MetaPage) {
	kv.tree.WriteMetaPage(metaPage)
}

func (kv *KV) Get(metaPage MetaPage, key []byte) ([]byte, bool) {
	res := kv.tree.Find(metaPage, key)
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

func (kv *KV) GetRange(metaPage MetaPage, keyStart []byte, keyEnd []byte) ([][]byte, bool) {
	res := make([][]byte, 0)
	iter := kv.tree.SeekGE(metaPage, keyStart)
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

func (kv *KV) Set(metaPage MetaPage, key []byte, val []byte) MetaPage {
	return kv.tree.Set(metaPage, key, val)
}

func (kv *KV) Del(metaPage MetaPage, key []byte) (bool, MetaPage) {
	return kv.tree.Del(metaPage, key)
}

func (kv *KV) CommitToDisk() {
	// TODO: Rollback with snapshot in the beginning of the transaction
	for {
		committedTx := kv.history[0]
		// Need to commit
		kv.WriteMetaPage(committedTx.mt)
		kv.history = kv.history[1:]
	}
}
