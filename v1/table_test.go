package tinytable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPaths(t *testing.T) {
	tab := New[int]()
	cf := tab.CF("cols")

	var v int
	var a []int
	var ok bool

	// add some elements
	cf.Put("a", 100)
	cf.Put("b", 90)
	cf.Put("b1", 91)
	cf.Put("b2", 92)
	cf.Put("c", 234)

	// query them
	v, ok = cf.Get("a")
	if assert.Equal(t, true, ok) {
		assert.Equal(t, 100, v)
	}
	v, ok = cf.Get("b")
	if assert.Equal(t, true, ok) {
		assert.Equal(t, 90, v)
	}
	v, ok = cf.Get("c")
	if assert.Equal(t, true, ok) {
		assert.Equal(t, 234, v)
	}

	// this one doesn't exist
	v, ok = cf.Get("d")
	assert.Equal(t, false, ok)

	// iterate over every element
	a = make([]int, 0)
	cf.Every(func(k string, v int) bool {
		a = append(a, v)
		return true
	})
	if assert.Len(t, a, 5) {
		assert.Equal(t, 100, a[0])
		assert.Equal(t, 90, a[1])
		assert.Equal(t, 91, a[2])
		assert.Equal(t, 92, a[3])
		assert.Equal(t, 234, a[4])
	}

	// iterate over every element beginning at some point
	a = make([]int, 0)
	cf.Start("b", func(k string, v int) bool {
		a = append(a, v)
		return true
	})
	if assert.Len(t, a, 4) {
		assert.Equal(t, 90, a[0])
		assert.Equal(t, 91, a[1])
		assert.Equal(t, 92, a[2])
		assert.Equal(t, 234, a[3])
	}

	// iterate over every elements with a prefix
	a = make([]int, 0)
	cf.Prefix("b", func(k string, v int) bool {
		a = append(a, v)
		return true
	})
	if assert.Len(t, a, 3) {
		assert.Equal(t, 90, a[0])
		assert.Equal(t, 91, a[1])
		assert.Equal(t, 92, a[2])
	}

	// iterate over a specific range of items
	a = make([]int, 0)
	cf.Range("b", "c", func(k string, v int) bool {
		a = append(a, v)
		return true
	})
	if assert.Len(t, a, 3) {
		assert.Equal(t, 90, a[0])
		assert.Equal(t, 91, a[1])
		assert.Equal(t, 92, a[2])
	}

	// iterate over a specific range of items
	a = make([]int, 0)
	cf.Range("a", "c", func(k string, v int) bool {
		a = append(a, v)
		return true
	})
	if assert.Len(t, a, 4) {
		assert.Equal(t, 100, a[0])
		assert.Equal(t, 90, a[1])
		assert.Equal(t, 91, a[2])
		assert.Equal(t, 92, a[3])
	}

	// delete an item
	ok = cf.Delete("a")
	assert.Equal(t, true, ok)

	// query the deleted item (which shouldn't exist)
	v, ok = cf.Get("a")
	assert.Equal(t, false, ok)

	// iterate over a specific range of items
	a = make([]int, 0)
	cf.Range("a", "c", func(k string, v int) bool {
		a = append(a, v)
		return true
	})
	if assert.Len(t, a, 3) {
		assert.Equal(t, 90, a[0])
		assert.Equal(t, 91, a[1])
		assert.Equal(t, 92, a[2])
	}
}
