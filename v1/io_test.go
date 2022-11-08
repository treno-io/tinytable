package tinytable

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const snapshot1 = `{
	"a": {
		"1": 1,
		"2": 2,
		"3": 3
	},
	"b": {
		"100": 100,
		"200": 200,
		"300": 300
	}
}`

const snapshot2 = `{"a":{"1":1,"2":2,"3":3},"b":{"100":100,"200":200,"300":300},"c":{"777":777,"888":888,"999":999}}
`

func TestRestoreAndPersist(t *testing.T) {
	tab, err := Read[int](bytes.NewReader([]byte(snapshot1)))
	assert.NoError(t, err)

	var v int

	cf := tab.CF("a")
	v, _ = cf.Get("1")
	assert.Equal(t, 1, v)
	v, _ = cf.Get("2")
	assert.Equal(t, 2, v)
	v, _ = cf.Get("3")
	assert.Equal(t, 3, v)

	cf = tab.CF("b")
	v, _ = cf.Get("100")
	assert.Equal(t, 100, v)
	v, _ = cf.Get("200")
	assert.Equal(t, 200, v)
	v, _ = cf.Get("300")
	assert.Equal(t, 300, v)

	cf = tab.CF("c")
	cf.Put("999", 999)
	cf.Put("888", 888)
	cf.Put("777", 777)

	dst := &bytes.Buffer{}
	err = tab.Write(dst)
	assert.NoError(t, err)

	fmt.Println(">>> " + string(dst.Bytes()))
	assert.Equal(t, snapshot2, string(dst.Bytes()))

	vfy, err := Read[int](dst)
	assert.NoError(t, err)

	cf = vfy.CF("a")
	v, _ = cf.Get("1")
	assert.Equal(t, 1, v)
	v, _ = cf.Get("2")
	assert.Equal(t, 2, v)
	v, _ = cf.Get("3")
	assert.Equal(t, 3, v)

	cf = tab.CF("b")
	v, _ = cf.Get("100")
	assert.Equal(t, 100, v)
	v, _ = cf.Get("200")
	assert.Equal(t, 200, v)
	v, _ = cf.Get("300")
	assert.Equal(t, 300, v)

	cf = vfy.CF("c")
	v, _ = cf.Get("999")
	assert.Equal(t, 999, v)
	v, _ = cf.Get("888")
	assert.Equal(t, 888, v)
	v, _ = cf.Get("777")
	assert.Equal(t, 777, v)
}
