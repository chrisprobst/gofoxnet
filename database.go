package gofoxnet

import (
	"errors"
	"fmt"
)

const emptyHash = ""

type chunk struct {
	hash        string
	buffer      []byte
	bufferIndex int
}

type metadata struct {
	hash        string
	splitHashes []string
}

type lookup struct {
	hash    string
	resChan chan mergeResult
}

type mergeResult struct {
	buffer []byte
	err    error
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type dataset struct {
	metadata
	chunks      map[int][]byte
	mergeResult *mergeResult
}

func (d *dataset) ensureChunkCount() bool {
	return len(d.chunks) == len(d.splitHashes)
}

func (d *dataset) merge() mergeResult {
	if !d.ensureChunkCount() {
		panic("Invalid chunk count")
	}

	// Calc length of all buffers
	s := 0
	for _, c := range d.chunks {
		s += len(c)
	}

	m := make([]byte, 0, s)
	for i, h := range d.splitHashes {
		c, ok := d.chunks[i]

		// Make sure, there the chunk exists
		if !ok {
			return mergeResult{nil, errors.New(fmt.Sprint("Chunk with index", i, "not found"))}
		}

		// Verify the split hash
		if h != hash(c) {
			return mergeResult{nil, errors.New(fmt.Sprint("Chunk", i, "corrupted"))}
		}

		// Append to merged buffer
		m = append(m, c...)
	}

	// Verify hash
	if d.hash != hash(m) {
		return mergeResult{nil, errors.New(fmt.Sprint("All chunks corrupted"))}
	}

	return mergeResult{m, nil}
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

type database struct {
	// Used to communicate with the database
	addChunkChan    chan chunk
	addMetaDataChan chan metadata
	lookupChan      chan lookup
	done            signalChan
	closed          signalChan

	// Structures for storing chunks and datasets
	chunks   map[string]map[int]chunk
	datasets map[string]*dataset
	lookups  map[string][]lookup
}

// Try to merge the database and store the result
func (d *database) mergeAndNotify(ds *dataset) {
	// Not enough to merge
	if !ds.ensureChunkCount() {
		return
	}

	// Merge and notify
	res := ds.merge()

	// Store the merge result
	ds.mergeResult = &res

	// Notify listeners
	l := d.lookups[ds.hash]
	d.lookups[ds.hash] = nil
	for _, v := range l {
		select {
		case v.resChan <- res:
		case <-d.done:
		}
	}
}

func newDatabase() *database {
	d := &database{
		make(chan chunk),
		make(chan metadata),
		make(chan lookup),
		make(signalChan),
		make(signalChan),
		make(map[string]map[int]chunk),
		make(map[string]*dataset),
		make(map[string][]lookup),
	}
	go d.serve()
	return d
}

func (d *database) close() error {
	close(d.done)
	return nil
}

func (d *database) closeAndWait() error {
	err := d.close()
	<-d.closed
	return err
}

func (d *database) addChunk(c chunk) {
	select {
	case d.addChunkChan <- c:
	case <-d.done:
	}
}

func (d *database) addMetaData(md metadata) {
	select {
	case d.addMetaDataChan <- md:
	case <-d.done:
	}
}

func (d *database) lookup(hash string) ([]byte, error) {
	l := lookup{hash, make(chan mergeResult)}

	// Try to request lookup
	select {
	case d.lookupChan <- l:
	case <-d.done:
		return nil, errors.New("Database stopped while requesting lookup")
	}

	// Try to fetch result
	select {
	case res := <-l.resChan:
		return res.buffer, res.err
	case <-d.done:
		return nil, errors.New("Database stopped while waiting for lookup result")
	}
}

func (d *database) serve() {
	defer close(d.closed)
	for running := true; running; {
		select {
		case c := <-d.addChunkChan:
			if ds, ok := d.datasets[c.hash]; ok {
				// Datasets already exists
				ds.chunks[c.bufferIndex] = c.buffer
				d.mergeAndNotify(ds)
			} else if m, ok := d.chunks[c.hash]; ok {
				// There are chunks with the same hash
				m[c.bufferIndex] = c
			} else {
				// Add new chunks map
				d.chunks[c.hash] = map[int]chunk{c.bufferIndex: c}
			}
		case md := <-d.addMetaDataChan:
			if _, ok := d.datasets[md.hash]; !ok {
				// Create new dataset
				ds := &dataset{md, make(map[int][]byte), nil}
				d.datasets[md.hash] = ds

				// Merge outstanding chunks
				if m, ok := d.chunks[md.hash]; ok {
					for _, c := range m {
						ds.chunks[c.bufferIndex] = c.buffer
					}
				}
				d.mergeAndNotify(ds)

				// Remove chunk map for this dataset
				delete(d.chunks, md.hash)
			}
		case l := <-d.lookupChan:
			if ds, exists := d.datasets[l.hash]; exists && ds.mergeResult != nil {
				// Dataset exists and was already merged,
				// respond with merged result directly
				select {
				case l.resChan <- *ds.mergeResult:
				case <-d.done:
				}
			} else {
				// Queue for further notifications
				d.lookups[l.hash] = append(d.lookups[l.hash], l)
			}
		case <-d.done:
			running = false
			continue
		}
	}
}
