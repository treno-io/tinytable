package tinytable

import (
	"encoding/json"
	"io"
)

// Read a snapshot of a table from the specified reader.
func Read[T any](r io.Reader) (*Table[T], error) {
	var data map[string]map[string]T
	err := json.NewDecoder(r).Decode(&data)
	if err != nil {
		return nil, err
	}

	t := New[T]()
	for k, v := range data {
		cf := t.CF(k)
		for r, d := range v {
			cf.Put(r, d)
		}
	}

	return t, nil
}

// Write a snapshot of a table to the specified writer.
func (t *Table[T]) Write(w io.Writer) error {
	data := make(map[string]map[string]T)

	for k, cf := range t.cf {
		rows := make(map[string]T)
		cf.Every(func(k string, v T) bool {
			rows[k] = v
			return true
		})
		data[k] = rows
	}

	err := json.NewEncoder(w).Encode(data)
	if err != nil {
		return err
	}
	return nil
}
