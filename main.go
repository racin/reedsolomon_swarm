package main

import (
	"fmt"
	"github.com/klauspost/reedsolomon"
	"github.com/racin/entangle/entangler"
)

func main() {
	fmt.Println("Hello world!")
}

func upload(filepath string, filename string) {
	// read all of the contents of our uploaded file into a
	// byte array
	fileBytes, err := ioutil.ReadFile(filepath)
	if err != nil {
		fmt.Println(err)
	}

	if _, err := os.Create(entangler.TempDirectory + filename); err == nil {

	} else {
		fmt.Println("Fatal error ... " + err.Error())
		os.Exit(1)
	}
	ioutil.WriteFile(entangler.TempDirectory+filename, fileBytes, os.ModeAppend)

	// Chunker
	numChunks, err := entangler.ChunkFile(file)

	for i := 1; i <= numChunks; i++ {
		dataChunk, err := entangler.ReadChunk(ChunkDirectory + "d" + strconv.Itoa(i))
		if err != nil {
			return err
		}
	}

	entangler.EntangleFile(entangler.TempDirectory + filename)

	// Upload
	swarmconnector.UploadAllChunks()
}
