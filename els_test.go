package main

import (
	"testing"
)

func TestAppend(t *testing.T) {
	maxFilenameLength = 128
	els := NewELS("./buckets")
	b, err := els.OpenBucket("test")

	if err != nil {
		t.Error("Open bucket: " + err.Error())
		return
	}

	b.allocateDestroy()

	b.writeMode = MODE_APPEND
	data := []byte("AAAAAAAAAAAAAAAAA")
	_, err = b.Write(data)
	if err != nil {
		t.Error("Write: " + err.Error())
		return
	}
	_, err = b.Write(data)
	if err != nil {
		t.Error("Write: " + err.Error())
		return
	}

	v, _ := b.ReadValue()
	if len(v) != len(data)*2 {
		t.Error("Read wrong length: " + string(v))
	}
	if string(v) != string(data)+string(data) {
		t.Error("Read wrong content: " + string(v))
	}
}
