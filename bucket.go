package main

import (
	"encoding/base64"
	"errors"
	"io/ioutil"
	"os"
	"path"
)

type Bucket struct {
	bucketPath []bucketName
	name       bucketName
	fsPath     string
}

type dataFileName string

func (bkt *Bucket) allocate() error {
	err := os.RemoveAll(bkt.fsPath)
	if err != nil {
		return err
	}
	err = os.MkdirAll(bkt.fsPath, 0777)
	if err != nil {
		return err
	}
	return nil
}

func (bkt *Bucket) Write(buf []byte) (int64, error) {
	encdata := make([]byte, base64.URLEncoding.EncodedLen(len(buf)))
	base64.URLEncoding.Encode(encdata, buf)

	err := bkt.allocate()
	if err != nil {
		return 0, err
	}
	tmp := encdata
	filenames := make([]dataFileName, 0)
	dataEncoded := make([]int64, 0)
	for i := int64(0); len(tmp) > 0; i++ {
		fn := dataFileName("")
		x := int64(0)
		fn, tmp, x = encodeDataChunk(tmp, maxFilenameLength, i, base64.URLEncoding)
		filenames = append(filenames, fn)
		dataEncoded = append(dataEncoded, x)
	}
	dataWritten := int64(0)
	for i, file := range filenames {
		fpath := path.Join(bkt.fsPath, string(file))
		f, err := os.Create(fpath)
		f.Close()
		if err != nil {
			return dataWritten, err
		}
		dataWritten += dataEncoded[i]
	}
	return int64(len(buf)), nil
}

func (bkt *Bucket) IsValueBucket() (bool, error) {
	files, err := ioutil.ReadDir(bkt.fsPath)
	if err != nil {
		return false, err
	}
	for _, f := range files {
		if f.IsDir() {
			return false, nil
		}
	}
	return true, nil
}

func (bkt *Bucket) GetSubBuckets() ([]string, error) {
	files, err := ioutil.ReadDir(bkt.fsPath)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if !f.IsDir() {
			return nil, errors.New("This bucket contains valuesnot subbuckets")
		}
	}

	res := make([]string, len(files))
	for i, f := range files {
		res[i] = string(f.Name())
	}

	return res, nil
}

func (bkt *Bucket) Read(buf []byte) (int, error) {
	val, err := bkt.ReadValue()
	if err != nil {
		return 0, err
	}
	var i int
	for i = 0; i < len(buf) && i < len(val); i++ {
		buf[i] = val[i]
	}
	return i, nil
}

func (bkt *Bucket) ReadValue() ([]byte, error) {
	files, err := ioutil.ReadDir(bkt.fsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("No value written to Bucket")
		}
		return nil, err
	}
	if len(files) == 0 {
		return make([]byte, 0), nil
	}

	datas := make([][]byte, len(files))
	datalen := 0

	for _, f := range files {
		idx, data, err := decodeFileName(dataFileName(f.Name()))
		if err != nil {
			panic(err.Error())
		} else {
			datas[idx] = data
			datalen += len(data)
		}
	}

	padding := (4 - (datalen % 4))
	if padding == 4 {
		padding = 0
	}

	value := make([]byte, datalen+padding)
	validx := 0

	for _, data := range datas {
		for i, c := range data {
			value[validx+i] = c
		}
		validx += len(data)
	}

	for i := 0; i < padding; i++ {
		value[validx+i] = '='
	}

	buf := make([]byte, base64.URLEncoding.DecodedLen(len([]byte(value))))
	x, err := base64.URLEncoding.Decode(buf, []byte(value))
	if err != nil {
		return nil, err
	}
	buf = buf[:x]

	return buf, nil
}
