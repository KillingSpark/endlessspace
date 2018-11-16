package main

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"hash"
	"path/filepath"
	"strings"
)

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

var maxFilenameLength = int64(128)

func (els *ELS) mapBucketsToPath(Buckets []bucketName) string {
	path := ""
	for _, Bucket := range Buckets {
		path += encodeBucketName(els.hsh, base64.URLEncoding, Bucket) + "/"
	}
	return filepath.FromSlash(path)
}

func encodeBucketName(hsh hash.Hash, b64 *base64.Encoding, Bucket bucketName) string {
	hsh.Reset()
	hsh.Write([]byte(Bucket))
	hashed := hsh.Sum(nil)
	buf := make([]byte, base64.URLEncoding.EncodedLen(len(hashed)))
	b64.Encode(buf, hashed)
	encoded := string(buf)
	return encoded
}

func decodeFileName(file dataFileName) (int64, []byte, error) {
	sep := strings.Split(string(file), "=")
	switch len(sep[0]) % 4 {
	case 0:
		sep[0] += "===="
	case 1:
		sep[0] += "==="
	case 2:
		sep[0] += "=="
	case 3:
		sep[0] += "="
	}
	buf := make([]byte, base64.URLEncoding.DecodedLen(len([]byte(sep[0]))))
	x, err := base64.URLEncoding.Decode(buf, []byte(sep[0]))
	if err != nil {
		return 0, nil, err
	}
	buf = buf[:x]
	idx, x := binary.Varint(buf)
	if x <= 0 {
		return 0, nil, errors.New("Couldnt read Varint")
	}

	for i := len(sep) - 1; i > 0; i-- {
		if len(sep[i]) > 0 {
			return idx, []byte(sep[i]), nil
		}
	}
	return idx, nil, nil
}

func encodeDataChunk(data []byte, maxlen, index int64, b64 *base64.Encoding) (dataFileName, []byte, int64) {
	dataFile := dataFileName("")
	encidx := make([]byte, 32)
	x := binary.PutVarint(encidx, index)
	encidx = encidx[0:x]
	encidx2 := make([]byte, base64.URLEncoding.EncodedLen(len(encidx)))
	b64.Encode(encidx2, encidx)
	stridx := strings.Trim(string(encidx2), "\000")

	dataEncoded := min(maxlen-int64(len(stridx)), int64(len(data)))
	dataFile = dataFileName(stridx + string(data[:dataEncoded]))

	return dataFile, data[dataEncoded:], dataEncoded
}
