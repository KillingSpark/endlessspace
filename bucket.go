package main

import (
	"encoding/base64"
	"errors"
	"io/ioutil"
	"os"
	"path"
)

type WriteMode int8

const (
	MODE_APPEND = iota
	MODE_WRITE
)

type Bucket struct {
	bucketPath []bucketName
	name       bucketName
	fsPath     string
	writeMode  WriteMode

	// for appending
	lastIndex     int64
	lastDataChunk []byte
	lastFileName  dataFileName
	appendInfoSet bool
}

type dataFileName string

func (bkt *Bucket) findLastDataChunk() error {
	files, err := ioutil.ReadDir(bkt.fsPath)
	if err != nil {
		return err
	}
	maxIDX := int64(0)
	var lastData []byte
	var lastFileName dataFileName
	for _, f := range files {
		idx, data, err := decodeFileName(dataFileName(f.Name()))
		if err != nil {
			return err
		}
		if idx >= maxIDX {
			maxIDX = idx
			lastData = data
			lastFileName = dataFileName(f.Name())
		}
	}
	bkt.lastIndex = maxIDX
	bkt.lastDataChunk = lastData
	bkt.lastFileName = lastFileName
	bkt.appendInfoSet = true
	if len(bkt.lastDataChunk) > 0 {
		padding := 4 - len(bkt.lastDataChunk)%4
		if padding == 4 {
			padding = 0
		}
		for i := 0; i < padding; i++ {
			bkt.lastDataChunk = append(bkt.lastDataChunk, '=')
		}
	}
	return nil
}

func (bkt *Bucket) allocateDestroy() error {
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

func (bkt *Bucket) allocate() error {
	err := os.MkdirAll(bkt.fsPath, 0777)
	if err != nil {
		return err
	}
	return nil
}

func (bkt *Bucket) Write(buf []byte) (int64, error) {
	return bkt.write(buf)
}

func (bkt *Bucket) write(buf []byte) (int64, error) {
	if bkt.writeMode == MODE_WRITE {
		err := bkt.allocateDestroy()
		if err != nil {
			return 0, err
		}
		bkt.lastIndex = 0
		bkt.lastDataChunk = nil
		bkt.lastFileName = dataFileName("")
	}
	if bkt.writeMode == MODE_APPEND {
		err := bkt.allocate()
		if err != nil {
			return 0, err
		}
		ok, err := bkt.IsValueBucket()
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, errors.New("This bucket contains sub buckets")
		}
		if !bkt.appendInfoSet {
			err := bkt.findLastDataChunk()
			if err != nil {
				return 0, err
			}
		}
	}

	var encdata []byte //:= make([]byte, base64.URLEncoding.EncodedLen(len(buf)))

	// merge new data with last chunk if appending
	if bkt.writeMode == MODE_APPEND {
		if len(bkt.lastFileName) > 0 { //only if old files exist
			lenChunkDecoded := base64.URLEncoding.DecodedLen(len(bkt.lastDataChunk))
			appLen := lenChunkDecoded + len(buf)
			app := make([]byte, appLen)

			n, err := base64.URLEncoding.Decode(app, bkt.lastDataChunk)
			if err != nil {
				panic(err.Error() + "      " + string(bkt.lastDataChunk))
			}

			actualAppLen := appLen - (lenChunkDecoded - n)
			app = app[:actualAppLen]
			copy(app[n:], buf)

			encdata = make([]byte, base64.URLEncoding.EncodedLen(len(app)))
			base64.URLEncoding.Encode(encdata, app)

			//remove old file
			err = os.Remove(path.Join(bkt.fsPath, string(bkt.lastFileName)))
			if err != nil {
				return 0, err
			}
		} else {
			encdata = make([]byte, base64.URLEncoding.EncodedLen(len(buf)))
			base64.URLEncoding.Encode(encdata, buf)
		}
	}
	if bkt.writeMode == MODE_WRITE {
		encdata = make([]byte, base64.URLEncoding.EncodedLen(len(buf)))
		base64.URLEncoding.Encode(encdata, buf)
	}

	tmp := encdata

	filenames := make([]dataFileName, 0)
	dataEncoded := make([]int64, 0)

	//write the data. bkt.lastIndex is set to 0 for writing instead of appending
	for i := bkt.lastIndex; len(tmp) > 0; i++ {
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
			if i > 0 {
				bkt.lastFileName = filenames[i-1]
				bkt.lastDataChunk = encdata[dataWritten-dataEncoded[i-1] : dataWritten]
				bkt.lastIndex = bkt.lastIndex + int64(i-1)
			}
			return dataWritten, err
		}
		dataWritten += dataEncoded[i]
	}
	i := len(filenames) - 1
	bkt.lastFileName = filenames[i]
	bkt.lastDataChunk = encdata[dataWritten-dataEncoded[i]:]
	bkt.lastIndex = bkt.lastIndex + int64(i)
	return dataWritten, nil
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
	copy(buf, val)
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
