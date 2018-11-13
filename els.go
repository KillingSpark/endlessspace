package main

import (
	"errors"
	"hash"
	"os"
	"path"

	"golang.org/x/crypto/sha3"
)

type ELS struct {
	hsh  hash.Hash
	root string
}

type bucketName string

func NewELS(root string) *ELS {
	e := &ELS{}
	e.hsh = sha3.New224()
	e.root = root
	return e
}

func (els *ELS) newBucket(Buckets ...bucketName) *Bucket {
	b := &Bucket{}
	b.fsPath = path.Join(els.root, els.mapBucketsToPath(Buckets))
	b.bucketPath = Buckets[:len(Buckets)-1]
	b.name = Buckets[len(Buckets)-1]
	return b
}

func (els *ELS) OpenNewBucket(name string, names ...string) (*Bucket, error) {
	a := make([]bucketName, 1)
	bn := bucketName(name)
	a[0] = bn
	for _, name := range names {
		x := bucketName(name)
		a = append(a, x)
	}
	bkt := els.newBucket(a...)
	info, err := os.Stat(bkt.fsPath)

	if err == nil {
		if !info.IsDir() {
			return nil, errors.New("This is a file not a bucket")
		}
		return nil, errors.New("Bucket exists already")
	}

	//err is not nil here
	if os.IsNotExist(err) {
		return bkt, nil
	}
	return nil, err
}

func (els *ELS) OpenBucket(name string, names ...string) (*Bucket, error) {
	a := make([]bucketName, 1)
	bn := bucketName(name)
	a[0] = bn
	for _, name := range names {
		x := bucketName(name)
		a = append(a, x)
	}
	bkt := els.newBucket(a...)
	info, err := os.Stat(bkt.fsPath)

	if err == nil {
		if !info.IsDir() {
			return nil, errors.New("This is a file not a bucket")
		}
		return bkt, nil
	}

	//err is not nil here
	if os.IsNotExist(err) {
		return bkt, nil
	}
	return nil, err
}
