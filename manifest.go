package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/klauspost/reedsolomon"
	"github.com/racin/entangle/entangler"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type RS_Manifest struct {
	Buckets     []*RS_Bucket
	DataRequest chan *DownloadRequest
	DataStream  chan *RS_Shard
}

type RS_Bucket struct {
	Position     int
	Datashards   int
	Parityshards int
	Shards       []*RS_Shard
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

func (manifest *RS_Manifest) createBuckets(conf map[string]string, keys []reflect.Value, dataShards, parityShards, failrate int) {
	buckets := make(map[int]*RS_Bucket)
	for _, key := range keys {
		keyStr := key.String()
		leftright := strings.Split(keyStr, "_")
		left, _ := strconv.Atoi(leftright[0])
		right, _ := strconv.Atoi(leftright[1])
		var ok bool
		if _, ok = buckets[left]; !ok {
			buckets[left] = &RS_Bucket{
				Position: left, Datashards: dataShards, Parityshards: parityShards,
				Shards: make([]*RS_Shard, 0, dataShards+parityShards)}
		}

		rnd := rand.Intn(100)
		var unavail bool
		if rnd < failrate {
			unavail = true
		}

		buckets[left].Shards = append(buckets[left].Shards, &RS_Shard{
			Identifier: conf[keyStr], Bucket: left, Position: right,
			IsUnavailable: unavail,
		})
	}
	for i := 1; i <= len(buckets); i++ {
		manifest.Buckets = append(manifest.Buckets, buckets[i])
	}
}

func NewRS_Manifest(confpath string, dataShards, parityShards int, datarequest chan *DownloadRequest) *RS_Manifest {
	return NewRS_ManifestWithFail(confpath, dataShards, parityShards, datarequest, 0)
}

func NewRS_ManifestWithFail(confpath string, dataShards, parityShards int, datarequest chan *DownloadRequest, failrate int) *RS_Manifest {
	conf, _ := entangler.LoadFileStructure(confpath)
	var manifest *RS_Manifest = &RS_Manifest{
		Buckets:     make([]*RS_Bucket, 0, len(conf)/(dataShards+parityShards)),
		DataRequest: datarequest,
		DataStream:  make(chan *RS_Shard, len(conf)),
	}
	manifest.createBuckets(conf, reflect.ValueOf(conf).MapKeys(), dataShards, parityShards, failrate)
	return manifest
}

func (s *RS_Shard) HasData() bool {
	return s != nil && s.Data != nil && len(s.Data) != 0
}

func (manifest *RS_Manifest) CanReconstruct() bool {
	for i := 0; i < len(manifest.Buckets); i++ {
		if !manifest.Buckets[i].CanReconstruct() {
			return false
		}
	}
	return true
}

func (manifest *RS_Manifest) Reconstruct(filepath string) error {
	bucketsFilled := true
	lenBucket := len(manifest.Buckets)
	if lenBucket == 0 {
		return errors.New("Bucket size is 0")
	}

	for i := 0; i < lenBucket; i++ {
		b := manifest.Buckets[i]
		if b.HasData() {
			continue
		} else if b.CanReconstruct() {
			bucketsFilled = bucketsFilled && b.Reconstruct() != nil
		} else {
			bucketsFilled = false
		}
	}

	if bucketsFilled {
		f, err := os.Create(filepath)
		if err != nil {
			return err
		}
		w := bufio.NewWriter(f)

		for i := 0; i < lenBucket; i++ {
			w.Write(manifest.Buckets[i].Data)
		}

		w.Flush()
		return nil
	}
	return errors.New("Could not reconstruct all buckets.")
}

func (s *RS_Shard) SetData(data []byte, start, end int64, wasDownload bool) {
	s.Data = data
	s.StartTime = start
	s.EndTime = end
	s.WasDownloaded = wasDownload
}

func (s *RS_Bucket) HasData() bool {
	return s != nil && s.Data != nil && len(s.Data) != 0
}

func (bucket *RS_Bucket) CanReconstruct() bool {
	if bucket.HasData() { // Already reconstructed.
		return true
	}

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
	if bucket.HasData() {
		return bucket.Data
	}

	if !bucket.CanReconstruct() {
		return nil
	}
	enc, err := reedsolomon.New(bucket.Datashards, bucket.Parityshards)
	if err != nil {
		return nil
	}

	shards := make([][]byte, len(bucket.Shards), len(bucket.Shards))

	for i := 0; i < len(bucket.Shards); i++ {
		shards[bucket.Shards[i].Position] = bucket.Shards[i].Data
	}

	err = enc.Reconstruct(shards)
	if err != nil {
		return nil
	}

	ok, err := enc.Verify(shards)
	if !ok || err != nil {
		return nil
	}

	buf := bytes.NewBuffer(make([]byte, 0, bucket.Datashards))
	enc.Join(buf, shards, len(shards[0])*bucket.Datashards)

	bucket.Data = buf.Bytes()

	return bucket.Data
}

func (b *RS_Shard) String() string {
	return fmt.Sprintf("Bucket: %d, Pos: %d, HasData: %t, WasDownloaded: %t, IsUnavailable: %t",
		b.Bucket, b.Position, b.HasData(),
		b.WasDownloaded, b.IsUnavailable)
}

// fmt.Printf("%t,%d,%d,%d,%t,%d,%d,%t\n", block.IsParity, block.Position, block.LeftPos(0), block.RightPos(0), block.HasData(), start, time.Now().UnixNano(), false)
func (b *RS_Shard) Log() string {
	return fmt.Sprintf("%d,%d,%t,%t,%t,%d,%d\n",
		b.Bucket, b.Position, b.HasData(),
		b.WasDownloaded, b.IsUnavailable,
		b.StartTime, b.EndTime)
}

func DebugPrint(format string, a ...interface{}) (int, error) {
	if false {
		return fmt.Printf(format, a...)
	}
	return 0, nil
}
