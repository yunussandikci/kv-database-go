package kvdatabase

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type kvDatabase[K comparable, V any] struct {
	cache *map[K]V
	file  *os.File
}

type KVDatabase[K comparable, V any] interface {
	Get(key K) (V, bool)
	Set(key K, value V)
	Read() error
	Flush()
	Persist() error
}

func New[K comparable, V any](filepath string) (KVDatabase[K, V], error) {
	file, openErr := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0666)
	if openErr != nil {
		panic(openErr)
	}
	instance := &kvDatabase[K, V]{
		file: file,
	}
	if readErr := instance.Read(); readErr != nil {
		return nil, readErr
	}
	return instance, nil
}

func (l *kvDatabase[K, V]) Get(key K) (V, bool) {
	value, exist := (*l.cache)[key]
	return value, exist
}

func (l *kvDatabase[K, V]) Set(key K, value V) {
	(*l.cache)[key] = value
}

func (l *kvDatabase[K, V]) Read() error {
	fileContent, readErr := ioutil.ReadAll(l.file)
	if readErr != nil {
		return readErr
	}
	if len(fileContent) == 0 {
		fileContent = []byte("{}")
	}
	var data map[K]V
	unmarshallErr := json.Unmarshal(fileContent, &data)
	if unmarshallErr != nil {
		return unmarshallErr
	}
	l.cache = &data
	return nil
}

func (l *kvDatabase[K, V]) Persist() error {
	data, marshallErr := json.Marshal(l.cache)
	if marshallErr != nil {
		return marshallErr
	}
	if truncateErr := l.file.Truncate(0); truncateErr != nil {
		return truncateErr
	}
	if _, seekErr := l.file.Seek(0, 0); seekErr != nil {
		return seekErr
	}
	_, writeErr := l.file.Write(data)
	return writeErr
}

func (l *kvDatabase[K, V]) Flush() {
	*l.cache = make(map[K]V)
}
