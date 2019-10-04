package main

//import requiered libraries
import (
	"encoding/json"
	"fmt"
	e "github.com/racin/entangle/entangler"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
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
	lattice      *e.Lattice       // Shared map of retrieved blocks
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
	e.DebugPrint("GOT DATA REQUEST. %v\n", block.String())

	if block.IsUnavailable {
		e.DebugPrint("UNAVAILABLE BLOCK %v\n", block.String())
		e.DebugPrint("Len left: %v, Len right: %v\n", len(block.Left[0].Left), len(block.Right[0].Right))
		fmt.Println("unexpected HTTP status: 404 Not Found")
		fmt.Printf("%t,%d,%d,%d,%t,%d,%d,%t\n", block.IsParity, block.Position, block.LeftPos(0), block.RightPos(0), block.HasData(), time.Now().UnixNano(), time.Now().UnixNano(), false)
		result <- block
		return
	}

	if block.HasData() {
		//block.DownloadStatus = 2
		e.DebugPrint("Block data already known. %v\n", block.String())
		result <- block
		return
	}

	if block.DownloadStatus != 0 {
		e.DebugPrint("Block download already queued. %v\n", block.String())
		result <- block
		return
	}
	start := time.Now().UnixNano()
	block.DownloadStatus = 1

	dl := p.reserve()

	content := make(chan []byte, 1) // Buffered chan is non-blocking
	//defer close(content)
	go func() {
		if file, err := dl.Client.Download(block.Identifier, ""); err == nil {
			if contentA, err := ioutil.ReadAll(file); err == nil {
				//block.DownloadStatus = 2
				e.DebugPrint("Completed download of block. %v\n", block.String())

				// Use Result if we get it.
				block.SetData(contentA, start, time.Now().UnixNano(), true)
				content <- contentA
				p.lattice.DataStream <- block

			}
		} else {
			fmt.Println(err.Error())
			fmt.Printf("%t,%d,%d,%d,%t,%d,%d,%t\n", block.IsParity, block.Position, block.LeftPos(0), block.RightPos(0), block.HasData(), start, time.Now().UnixNano(), false)
		}

		p.release(dl)
		block.DownloadStatus = 0

		// Dont use result
		//content <- contentA
	}()
	select {
	//case <-time.After(1 * time.Second):
	case <-time.After(5000 * time.Millisecond):
		//e.DebugPrint("TIMEOUT.%v\n", block.String())
		result <- block
	case <-content:
		if block.HasData() {
			result <- block
		}
	}
}
func (p *DownloadPool) DownloadFile(config, output string) error {
	// 1. Construct lattice
	lattice := e.NewLatticeWithFailure(e.Alpha, e.S, e.P, config, p.Datarequests, 15)
	p.lattice = lattice

	// 2. Attempt to download Data Blocks
	for i := 0; i < lattice.NumDataBlocks; i++ {
		go p.DownloadBlock(lattice.Blocks[i], lattice.DataStream)
	}

	datablocks, parityblocks := 0, 0
	// 3. Issue repairs if neccesary
	go func() {
		time.Sleep(120 * time.Second)
		fmt.Println("TIMEOUT - NOT REPAIRABLE LATTICE -- IGNORE")
		os.Exit(0)
	}()
repairs:
	for {
		select {
		case dl := <-lattice.DataStream:
			if dl == nil {
				// TODO: Print stack trace.
				fmt.Println("FATAL ERROR. STOPPING DOWNLOAD.")
				// Try new strategy?
				os.Exit(0)
				break repairs
			} else if !dl.HasData() {
				// repair
				//e.DebugPrint("Block was missing. Position: %d\n", dl.Position)
				if dl.Position < 6 || (dl.Position > 242 && dl.Position != 245) { // Closed lattice ..
					go p.DownloadBlock(dl, lattice.DataStream)
				} else {
					//go p.DownloadBlock(dl, lattice.DataStream)
					go lattice.HierarchicalRepair(dl, lattice.DataStream, make([]*e.Block, 0))
					//go lattice.RoundrobinRepair(dl, lattice.DataStream, make([]*e.Block, 0))
				}
				//go p.DownloadBlock(dl, lattice.DataStream)
			} else {
				e.DebugPrint("Download success. %v\n", dl.String())
				if !dl.IsParity && dl.DownloadStatus != 3 {
					dl.DownloadStatus = 3
					lattice.MissingDataBlocks--
					e.DebugPrint("Data block download success. Position: %d. Missing: %d\n", dl.Position, lattice.MissingDataBlocks)

					// Due to concurrency bug. TODO: Fix with lock, counter ?
					complete := true
					for i := 0; i < lattice.NumDataBlocks; i++ {
						if !lattice.Blocks[i].HasData() {
							complete = false
							//e.DebugPrint("BREAKING OUT....%v\n", lattice.Blocks[i])
							//go lattice.HierarchicalRepair(lattice.Blocks[i], lattice.DataStream, make([]*e.Block, 0))
							break
						}
					}

					if complete { //lattice.MissingDataBlocks == 0 {
						e.DebugPrint("Received all data blocks. Position: %d\n", dl.Position)
						for i := 0; i < len(lattice.Blocks); i++ {
							b := lattice.Blocks[i]
							if b.HasData() {
								if b.WasDownloaded {
									if b.IsParity {
										parityblocks++
									} else {
										datablocks++
									}
								}
								fmt.Printf("%t,%d,%d,%d,%t,%d,%d,%t\n", b.IsParity, b.Position,
									b.LeftPos(0), b.RightPos(0), b.HasData(), b.StartTime,
									b.EndTime, b.WasDownloaded)
							}
						}
						break repairs
					}
				}
			}
		}
	}
	// 4. Rebuild the file

	fmt.Printf("Downloaded total. Blocks: %d\n", blocks)
	return (output)
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
		e.DebugPrint("Generating new resource")
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
