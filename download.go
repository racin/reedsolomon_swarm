package main

//import requiered libraries
import (
	"fmt"
	e "github.com/racin/entangle/entangler"
	"io/ioutil"
	"os"
	"sync"
	"time"
	//bzzclient "https://github.com/ethereum/go-ethereum/tree/master/swarm/api/client/client.go"
	bzzclient "github.com/ethereum/go-ethereum/swarm/api/client"
)

type Downloader struct {
	Client *bzzclient.Client
}

type DownloadPool struct {
	lock         sync.Mutex       // Locking
	resource     chan *Downloader // Channel to obtain resource from the pool
	reedsolomon  *RS_Manifest     // Shared map of retrieved blocks
	Capacity     int              // Maximum capacity of the pool.
	count        int              // Current count of allocated resources.
	endpoint     string
	Datarequests chan *DownloadRequest
	datastream   chan *e.DownloadResponse
}

type DownloadRequest struct {
	Result chan *RS_Shard
	Block  *RS_Shard
}

func NewDownloadPool(capacity int, endpoint string) *DownloadPool {
	d := &DownloadPool{
		resource:     make(chan *Downloader, capacity),
		Datarequests: make(chan *DownloadRequest),
		Capacity:     capacity,
		count:        0,
		endpoint:     endpoint,
	}
	go func() {
		for {
			select {
			case request := <-d.Datarequests:
				go d.DownloadBlock(request.Block, request.Result)
			}
		}
	}()
	return d
}

func (p *DownloadPool) DownloadBlock(block *RS_Shard, result chan *RS_Shard) {
	DebugPrint("GOT DATA REQUEST. %v\n", block.String())

	if block.IsUnavailable {
		DebugPrint("UNAVAILABLE BLOCK %v\n", block.String())
		fmt.Println("unexpected HTTP status: 404 Not Found")
		fmt.Print(block.Log())
		result <- block
		return
	}

	if p.reedsolomon.Buckets[block.Bucket-1].CanReconstruct() {
		DebugPrint("Can already reconstruct bucket. Do not need to download. %v\n", block.String())
		result <- block
		return
	}

	if block.HasData() {
		//block.DownloadStatus = 2
		DebugPrint("Block data already known. %v\n", block.String())
		result <- block
		return
	}
	start := time.Now().UnixNano()

	dl := p.reserve()

	content := make(chan []byte, 1) // Buffered chan is non-blocking
	//defer close(content)
	go func() {
		if file, err := dl.Client.Download(block.Identifier, ""); err == nil {
			if contentA, err := ioutil.ReadAll(file); err == nil {
				// Use Result if we get it.
				block.SetData(contentA, start, time.Now().UnixNano(), true)
				DebugPrint("Completed download of block. %v\n", block.String())
				content <- contentA
				p.reedsolomon.DataStream <- block

			}
		} else {
			fmt.Println(err.Error())
			fmt.Print(block.Log())
		}

		p.release(dl)
	}()
	select {
	case <-time.After(1000 * time.Millisecond):
		result <- block
	case <-content:
		if block.HasData() {
			result <- block
		}
	}
}
func (p *DownloadPool) DownloadFile(config, output string) error {
	manifest := NewRS_Manifest(config, dataShards, parityShards, p.Datarequests)
	p.reedsolomon = manifest

	// 2. Attempt to download Data Blocks
	for i := 0; i < totalShards; i++ {
		for j := 0; j < len(manifest.Buckets); j++ {
			go p.DownloadBlock(manifest.Buckets[j].Shards[i], manifest.DataStream)
		}
	}

	blocks := 0
	// 3. Issue repairs if neccesary
	go func() {
		time.Sleep(120 * time.Second)
		fmt.Println("TIMEOUT - NOT REPAIRABLE DATA -- IGNORE")
		os.Exit(0)
	}()
repairs:
	for {
		select {
		case dl := <-manifest.DataStream:
			if dl == nil {
				// TODO: Print stack trace.
				fmt.Println("FATAL ERROR. STOPPING DOWNLOAD.")
				// Try new strategy?
				os.Exit(0)
				break repairs
			} else if !dl.HasData() {
				// repair
				DebugPrint("Block was missing. Position: %d\n", dl.Position)
			} else {
				DebugPrint("Download success. %v\n", dl.String())
				DebugPrint("Data block download success. Bucket: %d, Position: %d\n", dl.Bucket, dl.Position)

				if manifest.CanReconstruct() {
					for i := 0; i < totalShards; i++ {
						for j := 0; j < len(manifest.Buckets); j++ {
							b := manifest.Buckets[j].Shards[i]
							if b.HasData() {
								blocks++
								fmt.Print(b.Log())
							}
						}
					}
					break repairs
				}
			}
		}
	}
	// 4. Rebuild the file

	fmt.Printf("Downloaded total. Blocks: %d\n", blocks)
	return manifest.Reconstruct(output)
}

// Drain drains the pool until it has no more than n resources
func (p *DownloadPool) Drain(n int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for len(p.resource) > n {
		<-p.resource
		p.count--
	}
}

func (p *DownloadPool) reserve() *Downloader {
	p.lock.Lock()
	defer p.lock.Unlock()
	var d *Downloader
	if p.count == p.Capacity {
		return <-p.resource
	}
	select {
	case d = <-p.resource:
	default:
		DebugPrint("Generating new resource\n")
		d = newDownloader(p.endpoint)
		p.count++
	}
	return d
}

// release gives back a Downloader to the pool
func (p *DownloadPool) release(d *Downloader) {
	p.resource <- d
}

// Initizalites a new downloader
func newDownloader(endpoint string) *Downloader {
	return &Downloader{
		Client: bzzclient.NewClient(endpoint),
	}
}
