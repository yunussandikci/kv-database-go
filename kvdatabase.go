package main

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
	Read()
	Flush()
	Persist()
}

func NewKVDatabase[K comparable, V any](filepath string) KVDatabase[K, V] {
	file, openErr := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0666)
	if openErr != nil {
		panic(openErr)
	}
	localKVStorage := &kvDatabase[K, V]{
		file: file,
	}
	localKVStorage.Read()
	return localKVStorage
}

func (l *kvDatabase[K, V]) Get(key K) (V, bool) {
	value, exist := (*l.cache)[key]
	return value, exist
}

func (l *kvDatabase[K, V]) Set(key K, value V) {
	(*l.cache)[key] = value
}

func (l *kvDatabase[K, V]) Read() {
	fileContent, readErr := ioutil.ReadAll(l.file)
	if readErr != nil {
		panic(readErr)
	}
	if len(fileContent) == 0 {
		fileContent = []byte("{}")
	}
	var data map[K]V
	unmarshallErr := json.Unmarshal(fileContent, &data)
	if unmarshallErr != nil {
		panic(unmarshallErr)
	}
	l.cache = &data
}

func (l *kvDatabase[K, V]) Persist() {
	data, marshallErr := json.Marshal(l.cache)
	if marshallErr != nil {
		panic(marshallErr)
	}
	if truncateErr := l.file.Truncate(0); truncateErr != nil {
		panic(truncateErr)
	}
	if _, seekErr := l.file.Seek(0, 0); seekErr != nil {
		panic(seekErr)
	}
	_, writeErr := l.file.Write(data)
	if writeErr != nil {
		panic(writeErr)
	}
}

func (l *kvDatabase[K, V]) Flush() {
	*l.cache = make(map[K]V)
}
