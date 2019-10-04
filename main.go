package main

import (
	"bufio"
	"fmt"
	"github.com/klauspost/reedsolomon"
	"github.com/racin/entangle/entangler"
	"github.com/racin/entangle/swarmconnector"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

const (
	dataShards   = 4  //112 //4
	parityShards = 12 //16  //12
	totalShards  = dataShards + parityShards
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

	//upload()
	download()

	//_ = restructTest("file.jpeg")
}

func restructTest(filepath string) error {
	if err := readFile("resources/AlgardStasjon.jpg"); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	b := make([][]byte, 0, 0)
	for data, i := getData(1), 1; len(data) > 0; data = getData(i) {
		b = append(b, data)
		i++
	}

	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)

	for i := 0; i < len(b); i++ {
		w.Write(b[i])
	}
	w.Flush()
	return nil
}
func download() {
	dp := NewDownloadPool(100, "https://swarm-gateways.net")
	t := time.Now().UnixNano()
	err := dp.DownloadFile("resources/retrives.txt", "files/main_"+fmt.Sprintf("%d", t)+".jpeg")
	fmt.Println("Downloaded file")
	if err != nil {
		fmt.Println(err.Error())
	}

	fmt.Printf("%d,%d\n", t, time.Now().UnixNano())
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
	maxI := min(len(fileBytes), sliceSize*index)
	return fileBytes[minI:maxI]
}

func upload() {
	if err := readFile("resources/AlgardStasjon.jpg"); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	for data, i := getData(1), 1; len(data) > 0; data = getData(i) {
		enc, err := reedsolomon.New(dataShards, parityShards)
		checkErr(err)

		idata := make([]byte, len(data))
		copy(idata, data)
		split, err := enc.Split(idata)
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

		for j := 0; j < len(split); j++ {
			WriteChunkToFile(split[j], i, j)
		}
		i++
	}

	swarmconnector.UploadAllChunks()
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
