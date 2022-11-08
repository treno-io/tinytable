package tinytable

import (
	"log"
	"sync"

	"github.com/google/btree"
)

const logpfx = "tinytable: "

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
	conf Config
	cf   map[string]*CF[T]
}

func New[T any](opts ...Option) *Table[T] {
	return NewWithConfig[T](ConfigFromOptions(opts...))
}

func NewWithConfig[T any](conf Config) *Table[T] {
	return &Table[T]{
		conf: conf,
		cf:   make(map[string]*CF[T]),
	}
}

func (t *Table[T]) CF(name string) *CF[T] {
	cf, ok := t.cf[name]
	if !ok {
		cf = &CF[T]{
			conf: t.conf,
			data: btree.NewG[Row[T]](2, less[T]),
		}
		t.cf[name] = cf
	}
	return cf
}

type CF[T any] struct {
	conf Config
	data *btree.BTreeG[Row[T]]
	zero T // the zero value
}

func (f *CF[T]) Get(k string) (T, bool) {
	r, ok := f.data.Get(Row[T]{Key: k})
	if f.conf.Debug > 0 {
		log.Printf(logpfx+"get/1: %v → [%v] %v", k, ok, r)
	}
	if ok {
		return r.Val, true
	} else {
		return f.zero, false
	}
}

func (f *CF[T]) Every(t Iterator[T]) {
	var i int
	f.data.Ascend(func(e Row[T]) bool {
		if f.conf.Debug > 0 {
			i++
			log.Printf(logpfx+"get/n: #%d %v → %v", i, e.Key, e.Val)
		}
		return t(e.Key, e.Val)
	})
}

func (f *CF[T]) Prefix(prefix string, t Iterator[T]) {
	f.Range(prefix, successor(prefix), t)
}

func (f *CF[T]) Range(start, end string, t Iterator[T]) {
	var i int
	if end == "" {
		f.data.AscendGreaterOrEqual(Row[T]{Key: start}, func(e Row[T]) bool {
			if f.conf.Debug > 0 {
				i++
				log.Printf(logpfx+"get/n: [%s..] #%d %v → %v", start, i, e.Key, e.Val)
			}
			return t(e.Key, e.Val)
		})
	} else {
		f.data.AscendRange(Row[T]{Key: start}, Row[T]{Key: end}, func(e Row[T]) bool {
			if f.conf.Debug > 0 {
				i++
				log.Printf(logpfx+"get/n: [%s..%s] #%d %v → %v", start, end, i, e.Key, e.Val)
			}
			return t(e.Key, e.Val)
		})
	}
}

func (f *CF[T]) Put(k string, v T) {
	f.data.ReplaceOrInsert(Row[T]{k, v})
	if f.conf.Debug > 0 {
		log.Printf(logpfx+"put/1: %v → %v", k, v)
	}
}

func (f *CF[T]) Delete(k string) bool {
	_, ok := f.data.Delete(Row[T]{Key: k})
	if f.conf.Debug > 0 {
		log.Printf(logpfx+"del/1: %v → [%v]", k, ok)
	}
	return ok
}
