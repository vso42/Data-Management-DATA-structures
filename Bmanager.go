package manager

import (
	"encoding/binary"
	"errors"
	"sync"
)

const (
	PageSize  = 4096 // 4KB pages
	MaxFrames = 100  // Buffer pool size
)

type PageID uint64

type bufferPage struct {
	pageID   PageID
	data     [PageSize]byte
	isDirty  bool
	pinCount int
	refbit   bool
}

type BufferManager struct {
	disk       map[PageID]*[PageSize]byte
	frames     map[PageID]*bufferPage
	pageTable  map[PageID]int
	clockHand  int
	mu         sync.Mutex
	nextPageID PageID
}

func NewBufferManager() *BufferManager {
	bm := &BufferManager{
		disk:      make(map[PageID]*[PageSize]byte),
		frames:    make(map[PageID]*bufferPage),
		pageTable: make(map[PageID]int),
	}

	for i := 0; i < MaxFrames; i++ {
		bm.frames[i] = &bufferPage{}
	}

	return bm
}

func (bm *BufferManager) PinPage(pageID PageID) (*BufferPage, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if idx, exists := bm.pageTable[pageID]; exists {
		frame := bm.frames[idx]
		frame.pinCount++
		frame.refBit = true
		return frame, nil
	}

	data, exists := bm.disk[pageID]
	if !exists {
		return nil, errors.New("page does not exist")
	}

	victimIdx, err := bm.findVictim()
	if err != nil {
		return nil, errors.New("buffer full")
	}
	victim := bm.frames[victimIdx]

	if victim.isDirty {
		bm.disk[victim.pageID] = &victim.data
	}

	*victim = BufferPage{
		pageID:   pageID,
		data:     *data,
		pinCount: 1,
		refBit:   true,
	}

	delete(bm.pageTable, victim.pageID)
	bm.pageTable[pageID] = victimIdx
	return victim, nil
}

func (bm *BufferManager) findVictim() (int, error) {
	for i := 0; i < 2*MaxFrames; i++ {
		idx := (bm.clockHand + i) % MaxFrames
		frame := bm.frames[idx]

		if frame.pinCount > 0 {
			continue
		}

		if frame.refBit {
			frame.refBit = false
			continue
		}

		bm.clockHand = (idx + 1) % MaxFrames
		return idx, nil
	}
	return 0, errors.New("all pages pinned")
}

func (bm *BufferManager) UnpinPage(pageID PageID, isDirty bool) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	idx, exists := bm.pageTable[pageID]
	if !exists {
		return errors.New("page not in buffer")
	}

	frame := bm.frames[idx]
	frame.pinCount--
	if frame.pinCount < 0 {
		panic("pin count negative")
	}
	frame.isDirty = frame.isDirty || isDirty
	return nil
}

func (bm *BufferManager) FlushPage(pageID PageID) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if idx, exists := bm.pageTable[pageID]; exists {
		frame := bm.frames[idx]
		if frame.isDirty {
			// Write to disk
			bm.disk[pageID] = &frame.data
			frame.isDirty = false
		}
	}
	return nil
}

func (bm *BufferManager) NewPage() (PageID, *[PageSize]byte, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	pageID := bm.nextPageID
	bm.nextPageID++

	victimIdx, err := bm.findVictim()
	if err != nil {
		return 0, nil, errors.New("buffer full")
	}
	victim := bm.frames[victimIdx]

	if victim.isDirty {
		bm.disk[victim.pageID] = &victim.data
	}

	*victim = BufferPage{
		pageID:   pageID,
		pinCount: 1,
		isDirty:  true,
		refBit:   true,
	}

	delete(bm.pageTable, victim.pageID)
	bm.pageTable[pageID] = victimIdx
	return pageID, &victim.data, nil
}

func Sizzle(pageID PageID) [8]byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(pageID))
	return buf
}

func Unsizzle(buf [8]byte) PageID {
	return PageID(binary.BigEndian.Uint64(buf[:]))
}
