package main

import (
	"fmt"
	"github.com/klauspost/reedsolomon"
	"github.com/racin/entangle/entangler"
	"io/ioutil"
	"os"
	"strconv"
)

const (
	dataShards   = 112 //4
	parityShards = 16  //12
)

var (
	fileBytes []byte
)

func readFile(filepath string) error {
	var err error
	fileBytes, err = ioutil.ReadFile(filepath)
	return err
}
func main() {
	fmt.Println("Hello world!")
	if err := readFile("resources/AlgardStasjon.jpg"); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	upload()
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}
func checkErr(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func getData(index int) []byte {
	sliceSize := 4096 * (dataShards)
	minI := min(len(fileBytes), max(0, sliceSize*(index-1)))
	return fileBytes[minI:min(len(fileBytes), sliceSize*index)]
}

func upload() {
	chunks := make([][]byte, 0, len(fileBytes)/4096)
	for data, i := getData(1), 1; len(data) > 0; data, i = getData(i), i+1 {
		enc, err := reedsolomon.New(dataShards, parityShards)
		checkErr(err)

		split, err := enc.Split(data)
		checkErr(err)

		err = enc.Encode(split)
		checkErr(err)

		err = enc.Reconstruct(split)
		checkErr(err)

		ok, err := enc.Verify(split)
		checkErr(err)

		if !ok {
			fmt.Println("COULD NOT VERIFY??")
			os.Exit(1)
		}

		chunks = append(chunks, split[:]...)

		for j := 0; j < len(split); j++ {
			WriteChunkToFile(split[j], i, j)
		}
	}

	fmt.Println("OK")
	//	file, err := os.Open(filepath)
	// if err != nil {
	// 	os.Exit(1)
	// }

	// // Chunker
	// numChunks, err := entangler.ChunkFile(file)

	// // Upload
	// swarmconnector.UploadAllChunks()
}

func WriteChunkToFile(data []byte, bucket, index int) {
	filename := strconv.Itoa(bucket) + "_" + strconv.Itoa(index)
	if _, err := os.Create(entangler.ChunkDirectory + filename); err == nil {

	} else {
		fmt.Println("Fatal error ... " + err.Error())
		os.Exit(1)
	}

	ioutil.WriteFile(entangler.ChunkDirectory+filename, data, os.ModeAppend)
}
