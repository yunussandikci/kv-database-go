package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type KVDatabase[T any] interface {
	Read() error
	Get() (*map[string]T, error)
	GetByKey(key string) T
	Set(key string, value T)
	Persist() error
}

type kvDatabase[T any] struct {
	cache *map[string]T
	file  *os.File
}

func NewKVDatabase[T any](filepath string) (*kvDatabase[T], error) {
	file, openErr := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0666)
	if openErr != nil {
		return nil, openErr
	}
	localKVStorage := &kvDatabase[T]{
		file: file,
	}
	readErr := localKVStorage.Read()
	if readErr != nil {
		return nil, readErr
	}
	return localKVStorage, nil
}

func (l *kvDatabase[T]) Read() error {
	fileContent, readErr := ioutil.ReadAll(l.file)
	if readErr != nil {
		return readErr
	}
	if len(fileContent) == 0 {
		fileContent = []byte("{}")
	}
	var data map[string]T
	unmarshallErr := json.Unmarshal(fileContent, &data)
	if unmarshallErr != nil {
		return unmarshallErr
	}
	l.cache = &data
	return nil
}

func (l *kvDatabase[T]) Get() (*map[string]T, error) {
	return l.cache, nil
}

func (l *kvDatabase[T]) GetByKey(key string) T {
	return (*l.cache)[key]
}

func (l *kvDatabase[T]) Set(key string, value T) {
	(*l.cache)[key] = value
}

func (l *kvDatabase[T]) Persist() error {
	data, marshallErr := json.Marshal(l.cache)
	if marshallErr != nil {
		return marshallErr
	}
	_, writeErr := l.file.Write(data)
	if writeErr != nil {
		return writeErr
	}
	return nil
}
