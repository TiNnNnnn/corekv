package storage_eng

import (
	"corekv"
	"corekv/utils"
	"encoding/binary"
	"errors"
	"time"
)

type CoreKvStore struct {
	Path string
	db   *corekv.DB
}

func MakeCoreKvStore(path string) (*CoreKvStore, error) {
	opt := &corekv.Options{
		WorkDir:          "./work_test",
		SSTableMaxSz:     2*(1 << 30),
		MemTableSize:     1 << 20,
		ValueLogFileSize: 1 << 30,
		ValueThreshold:   4 * (1 << 10),
		MaxBatchCount:    10,
		MaxBatchSize:     1 << 20,
	}
	db := corekv.Open(opt)
	if db == nil {
		return nil, errors.New("create kv storage failed")
	}
	return &CoreKvStore{
		Path: path,
		db:   db,
	}, nil
}

func (corekv *CoreKvStore) PutBytesKv(k []byte, v []byte) error {
	e := utils.NewEntry(k, v).WithTTL(1000 * time.Second)
	return corekv.db.Set(e)
}

func (corekv *CoreKvStore) DeleteBytesK(k []byte) error {
	return corekv.db.Del(k)
}

func (corekv *CoreKvStore) GetBytesValue(k []byte) ([]byte, error) {
	e, error := corekv.db.Get(k)
	if error != nil {
		return nil, error
	}
	return e.Value, nil
}

func (corekv *CoreKvStore) Put(k string, v string) error {
	e := utils.NewEntry([]byte(k), []byte(v)).WithTTL(1000 * time.Second)
	return corekv.db.Set(e)
}

func (corekv *CoreKvStore) Get(k string) (string, error) {
	v, err := corekv.db.Get([]byte(k))
	if err != nil {
		return "", err
	}
	return string(v.Value), nil
}

func (corekv *CoreKvStore) Delete(k string) error {
	return corekv.db.Del([]byte(k))
}

func (corekv *CoreKvStore) DumpPrefixKey(prefix string) (map[string]string, error) {
	kvs := make(map[string]string)
	iter := corekv.db.NewIterator(&utils.Options{
		Prefix: []byte(prefix),
		IsAsc:  false,
	})
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		k := string(iter.Item().Entry().Key)
		v := string(iter.Item().Entry().Value)
		kvs[k] = v
	}
	if iter.Valid() {
		return kvs, nil
	} else {
		return kvs, errors.New("error iterator while dump prefixkey")
	}
}

func (corekv *CoreKvStore) FlushDB() {

}

func (corekv *CoreKvStore) SeekPrefixLast(prefix []byte) ([]byte, []byte, error) {
	iter := corekv.db.NewIterator(&utils.Options{
		Prefix: []byte(prefix),
		IsAsc:  false,
	})
	list := []utils.Entry{}
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		list = append(list, *utils.NewEntry(iter.Item().Entry().Key, iter.Item().Entry().Value))
	}
	var keyBytes, valBytes []byte

	if len(list) != 0 {
		keyBytes = list[len(list)-1].Key
		valBytes = list[len(list)-1].Value
	}
	return keyBytes, valBytes, nil
}

func (corekv *CoreKvStore) SeekPrefixKeyIdMax(prefix []byte) (uint64, error) {
	iter := corekv.db.NewIterator(&utils.Options{
		Prefix: []byte(prefix),
		IsAsc:  false,
	})
	defer iter.Close()
	var maxKeyId uint64
	maxKeyId = 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		if iter.Valid() == false {
			return maxKeyId, errors.New("iter invalid")
		}
		kBytes := iter.Item().Entry().Key
		KeyId := binary.LittleEndian.Uint64(kBytes[len(prefix):])
		if KeyId > maxKeyId {
			maxKeyId = KeyId
		}
	}
	return maxKeyId, nil
}

func (corekv *CoreKvStore) SeekPrefixFirst(prefix string) ([]byte, []byte, error) {
	iter := corekv.db.NewIterator(&utils.Options{
		Prefix: []byte(prefix),
		IsAsc:  false,
	})
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		return iter.Item().Entry().Key, iter.Item().Entry().Value, nil
	}
	return []byte{}, []byte{}, errors.New("seek not find key")
}

func (corekv *CoreKvStore) DelPrefixKeys(prefix string) error {
	iter := corekv.db.NewIterator(&utils.Options{
		Prefix: []byte(prefix),
		IsAsc:  false,
	})
	for iter.Rewind(); iter.Valid(); iter.Next() {
		err := corekv.db.Del(iter.Item().Entry().Key)
		if err != nil {
			return err
		}
	}
	iter.Close()
	return nil
}
