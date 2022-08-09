package tinytable

import (
	"sync"

	"github.com/google/btree"
)

type Row[T any] struct {
	Key string
	Val T
}

func less[T any](a, b Row[T]) bool {
	return a.Key < b.Key
}

type Table[T any] struct {
	sync.RWMutex
	cf map[string]*btree.BTreeG[Row[T]]
}

func New[T any]() *Table[T] {
	return &Table[T]{
		cf: make(map[string]*btree.BTreeG[Row[T]]),
	}
}

func (t *Table[T]) CF(name string) *CF[T] {
	cf, ok := t.cf[name]
	if !ok {
		cf = btree.NewG[Row[T]](2, less[T])
		t.cf[name] = cf
	}
	return &CF[T]{cf}
}

type CF[T any] struct {
	data *btree.BTreeG[Row[T]]
}

func (f *CF[T]) Get(k string) (T, bool) {
	r, ok := f.Get(k)
	return r, ok
}

func (f *CF[T]) Put(k string, v T) {
	f.Store(Row[T]{k, v})
}

func (f *CF[T]) Store(r Row[T]) {
	f.data.ReplaceOrInsert(r)
}

func (f *CF[T]) Delete(k string) (T, bool) {
	r, ok := f.Get(k)
	return r, ok
}
