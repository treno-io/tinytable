package tinytable

import (
	"sync"

	"github.com/google/btree"
)

type Iterator[T any] func(string, T) bool

func successor(k string) string {
	if l := len(k); l == 0 {
		return ""
	} else {
		return k[:l-1] + string(k[l-1]+1)
	}
}

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
	return &CF[T]{
		data: cf,
	}
}

type CF[T any] struct {
	data *btree.BTreeG[Row[T]]
	zero T // the zero value
}

func (f *CF[T]) Get(k string) (T, bool) {
	r, ok := f.data.Get(Row[T]{Key: k})
	if ok {
		return r.Val, true
	} else {
		return f.zero, false
	}
}

func (f *CF[T]) Every(t Iterator[T]) {
	f.data.Ascend(func(e Row[T]) bool {
		return t(e.Key, e.Val)
	})
}

func (f *CF[T]) Prefix(prefix string, t Iterator[T]) {
	f.Range(prefix, successor(prefix), t)
}

func (f *CF[T]) Range(start, end string, t Iterator[T]) {
	if end == "" {
		f.data.AscendGreaterOrEqual(Row[T]{Key: start}, func(e Row[T]) bool {
			return t(e.Key, e.Val)
		})
	} else {
		f.data.AscendRange(Row[T]{Key: start}, Row[T]{Key: end}, func(e Row[T]) bool {
			return t(e.Key, e.Val)
		})
	}
}

func (f *CF[T]) Put(k string, v T) {
	f.data.ReplaceOrInsert(Row[T]{k, v})
}

func (f *CF[T]) Delete(k string) bool {
	_, ok := f.data.Delete(Row[T]{Key: k})
	return ok
}
