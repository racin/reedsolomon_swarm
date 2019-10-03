package main

import (
	"bytes"
	"fmt"
	"github.com/klauspost/reedsolomon"
	"github.com/racin/entangle/entangler"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type RS_Manifest struct {
	Buckets []RS_Bucket
}

type RS_Bucket struct {
	Position     int
	Datashards   int
	Parityshards int
	Shards       []RS_Shard
	Data         []byte
}

type RS_Shard struct {
	Identifier    string
	Data          []byte
	Bucket        int
	Position      int
	WasDownloaded bool
	IsUnavailable bool
	StartTime     int64
	EndTime       int64
}

func (manifest *RS_Manifest) createBuckets(conf map[string]string, keys []reflect.Value, dataShards, parityShards int) {
	buckets := make(map[int]*RS_Bucket)
	for _, key := range keys {
		keyStr := key.String()
		leftright := strings.Split(keyStr, "_")
		left, _ := strconv.Atoi(leftright[0])
		right, _ := strconv.Atoi(leftright[1])
		var bucket *RS_Bucket
		var ok bool
		if _, ok = buckets[left]; !ok {
			buckets[left] = &RS_Bucket{
				Position: left, Datashards: dataShards, Parityshards: parityShards,
				Shards: make([]RS_Shard, 0, dataShards+parityShards)}
		}
		buckets[left].Shards = append(buckets[left].Shards, RS_Shard{
			Identifier: conf[keyStr], Bucket: left, Position: right,
		})
	}
}

func ParseRS_Manifest(confpath string, dataShards, parityShards int) *RS_Manifest {
	conf, _ := entangler.LoadFileStructure(confpath)
	var manifest *RS_Manifest
	manifest.createBuckets(conf, reflect.ValueOf(conf).MapKeys(), dataShards, parityShards)
	return &RS_Manifest{}
}

func (s *RS_Shard) HasData() bool {
	return s != nil && s.Data != nil && len(s.Data) != 0
}

func (bucket *RS_Bucket) CanReconstruct() bool {
	j := bucket.Datashards
	for i := 0; i < len(bucket.Shards); i++ {
		if bucket.Shards[i].HasData() {
			j--
		}
		if j == 0 {
			return true
		}
	}
	return false
}

func (bucket *RS_Bucket) Reconstruct() []byte {
	if !bucket.CanReconstruct() {
		return nil
	}
	enc, err := reedsolomon.New(bucket.Datashards, bucket.Parityshards)
	checkErr(err)

	shards := make([][]byte, 0, len(bucket.Shards))

	for i := 0; i < len(bucket.Shards); i++ {
		shards[bucket.Shards[i].Position-1] = bucket.Shards[i].Data
	}

	err = enc.Reconstruct(shards)
	checkErr(err)

	ok, err := enc.Verify(shards)
	checkErr(err)
	if !ok {
		fmt.Println("COULD NOT VERIFY??")
		os.Exit(1)
	}

	bucket.Data = make([]byte, 0, bucket.Datashards)
	buf := bytes.NewBuffer(bucket.Data)
	enc.Join(buf, shards, len(shards[0])*bucket.Datashards)

	return bucket.Data
}
