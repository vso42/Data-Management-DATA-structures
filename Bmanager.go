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
	data     []byte
	isDirty  bool
	pinCount int
}

type BufferManager struct {
	disk       map[PageID][]byte
	frames     map[PageID]*bufferPage
	freeList   []PageID
	mu         sync.Mutex
	nextPageID PageID
}

func NewBufferManager() *BufferManager {
	return &BufferManager{
		disk:     make(map[PageID][]byte),
		frames:   make(map[PageID]*bufferPage),
		freeList: make([]PageID, 0, MaxFrames),
	}
}

func Sizzle(pageID PageID) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(pageID))
	return buf
}

func Unsizzle(buf []byte) PageID {
	return PageID(binary.BigEndian.Uint64(buf))
}

func (bm *BufferManager) PinPage(pageID PageID) ([]byte, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if page, exists := bm.frames[pageID]; exists {
		page.pinCount++
		return page.data, nil
	}

	data, exists := bm.disk[pageID]
	if !exists {
		data = make([]byte, PageSize)
	}

	bm.frames[pageID] = &bufferPage{
		pageID:   pageID,
		data:     data,
		pinCount: 1,
	}
	return data, nil
}

func (bm *BufferManager) UnpinPage(pageID PageID, isDirty bool) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	page, exists := bm.frames[pageID]
	if !exists {
		return errors.New("page not found")
	}

	page.pinCount--
	page.isDirty = page.isDirty || isDirty

	if page.pinCount == 0 {
		bm.freeList = append(bm.freeList, pageID)
	}
	return nil
}

func (bm *BufferManager) FlushPage(pageID PageID) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	page, exists := bm.frames[pageID]
	if !exists {
		return errors.New("page not in buffer")
	}

	bm.disk[pageID] = page.data
	page.isDirty = false
	return nil
}

func (bm *BufferManager) NewPage() (PageID, []byte, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	pageID := bm.nextPageID
	bm.nextPageID++

	if len(bm.frames) >= MaxFrames {
		if len(bm.freeList) == 0 {
			return 0, nil, errors.New("buffer pool full")
		}
		evictID := bm.freeList[0]
		bm.freeList = bm.freeList[1:]
		if page, exists := bm.frames[evictID]; exists {
			if page.isDirty {
				bm.disk[evictID] = page.data
			}
			delete(bm.frames, evictID)
		}
	}

	data := make([]byte, PageSize)
	bm.frames[pageID] = &bufferPage{
		pageID:   pageID,
		data:     data,
		isDirty:  true,
		pinCount: 1,
	}
	return pageID, data, nil
}
