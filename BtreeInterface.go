package btree

import (
	"encoding/binary"
	"errors"
	"manager"
)

const (
	leafNode           = 0
	internalNode       = 1
	leafHeaderSize     = 32 // nodeType(8) + numKeys(8) + next(8) + prev(8)
	internalHeaderSize = 16 // nodeType(8) + numKeys(8)
	keySize            = 8
	valueSize          = 8
	ptrSize            = 8
)

type BTree struct {
	bm         *manager.BufferManager
	rootPageID manager.PageID
}

func NewBTree(bm *manager.BufferManager) *BTree {
	rootID, data, _ := bm.NewPage()
	initializeLeafPage(data)
	return &BTree{bm: bm, rootPageID: rootID}
}

func initializeLeafPage(data []byte) {
	binary.BigEndian.PutUint64(data[0:8], leafNode)
	binary.BigEndian.PutUint64(data[8:16], 0)  // numKeys = 0
	binary.BigEndian.PutUint64(data[16:24], 0) // next = 0
	binary.BigEndian.PutUint64(data[24:32], 0) // prev = 0
}

func initializeInternalPage(data []byte) {
	binary.BigEndian.PutUint64(data[0:8], internalNode)
	binary.BigEndian.PutUint64(data[8:16], 0) // numKeys = 0
}

func (bt *BTree) Insert(key, value uint64) error {
	return bt.insert(bt.rootPageID, key, value, nil, nil)
}

func (bt *BTree) insert(pageID manager.PageID, key, value uint64, leftChild, rightChild *manager.PageID) error {
	data, _ := bt.bm.PinPage(pageID)
	defer bt.bm.UnpinPage(pageID, true)

	nodeType := binary.BigEndian.Uint64(data[0:8])
	if nodeType == leafNode {
		return bt.insertLeaf(data, pageID, key, value)
	} else {
		return bt.insertInternal(data, pageID, key, value, leftChild, rightChild)
	}
}

func (bt *BTree) insertLeaf(data []byte, pageID manager.PageID, key, value uint64) error {
	numKeys := binary.BigEndian.Uint64(data[8:16])
	maxEntries := (PageSize - leafHeaderSize) / (keySize + valueSize)

	if numKeys < uint64(maxEntries) {
		return bt.insertIntoLeaf(data, key, value)
	}

	// Split leaf
	newPageID, newData, _ := bt.bm.NewPage()
	initializeLeafPage(newData)

	splitIndex := numKeys / 2
	copy(newData[leafHeaderSize:], data[leafHeaderSize+splitIndex*(keySize+valueSize):])

	// Update metadata
	binary.BigEndian.PutUint64(newData[8:16], numKeys-splitIndex) // New numKeys
	binary.BigEndian.PutUint64(data[8:16], splitIndex)            // Old numKeys

	// Update linked list
	nextPageID := binary.BigEndian.Uint64(data[16:24])
	binary.BigEndian.PutUint64(newData[16:24], nextPageID)
	binary.BigEndian.PutUint64(newData[24:32], uint64(pageID))
	binary.BigEndian.PutUint64(data[16:24], uint64(newPageID))

	// Insert split key into parent
	splitKey := binary.BigEndian.Uint64(data[leafHeaderSize+splitIndex*(keySize+valueSize):])
	return bt.insertIntoParent(pageID, splitKey, newPageID)
}

func (bt *BTree) insertIntoParent(oldPageID manager.PageID, splitKey uint64, newPageID manager.PageID) error {
	parentPageID := bt.findParent(bt.rootPageID, oldPageID)
	if parentPageID == 0 {
		// Create new root
		newRootID, newRootData, _ := bt.bm.NewPage()
		initializeInternalPage(newRootData)
		binary.BigEndian.PutUint64(newRootData[8:16], 1) // numKeys = 1
		copy(newRootData[internalHeaderSize:], manager.Sizzle(oldPageID))
		binary.BigEndian.PutUint64(newRootData[internalHeaderSize+ptrSize:], splitKey)
		copy(newRootData[internalHeaderSize+ptrSize+keySize:], manager.Sizzle(newPageID))
		bt.rootPageID = newRootID
		return nil
	}

	data, _ := bt.bm.PinPage(parentPageID)
	defer bt.bm.UnpinPage(parentPageID, true)

	numKeys := binary.BigEndian.Uint64(data[8:16])
	maxKeys := (PageSize - internalHeaderSize - ptrSize) / (keySize + ptrSize)

	if numKeys < uint64(maxKeys) {
		return bt.insertIntoInternal(data, parentPageID, splitKey, newPageID)
	}

	// Split internal node
	newInternalID, newInternalData, _ := bt.bm.NewPage()
	initializeInternalPage(newInternalData)

	splitIndex := numKeys / 2
	splitKeyInternal := binary.BigEndian.Uint64(data[internalHeaderSize+splitIndex*(ptrSize+keySize)+ptrSize:])

	// Copy entries to new internal node
	copy(newInternalData[internalHeaderSize:], data[internalHeaderSize+splitIndex*(ptrSize+keySize)+ptrSize+keySize:])
	binary.BigEndian.PutUint64(newInternalData[8:16], numKeys-splitIndex-1)

	// Update old internal node
	binary.BigEndian.PutUint64(data[8:16], splitIndex)

	// Promote splitKeyInternal
	return bt.insertIntoParent(parentPageID, splitKeyInternal, newInternalID)
}

func (bt *BTree) insertIntoInternal(data []byte, parentPageID manager.PageID, splitKey uint64, newPageID manager.PageID) error {
	numKeys := binary.BigEndian.Uint64(data[8:16])
	insertIndex := 0

	for ; insertIndex < int(numKeys); insertIndex++ {
		offset := internalHeaderSize + insertIndex*(ptrSize+keySize) + ptrSize
		currentKey := binary.BigEndian.Uint64(data[offset : offset+keySize])
		if splitKey < currentKey {
			break
		}
	}

	// Shift entries
	startOffset := internalHeaderSize + insertIndex*(ptrSize+keySize)
	bytesToShift := (int(numKeys) - insertIndex) * (ptrSize + keySize)
	if bytesToShift > 0 {
		copy(data[startOffset+(ptrSize+keySize):], data[startOffset:startOffset+bytesToShift])
	}

	// Insert new child and key
	copy(data[startOffset:], manager.Sizzle(newPageID))
	binary.BigEndian.PutUint64(data[startOffset+ptrSize:], splitKey)

	binary.BigEndian.PutUint64(data[8:16], numKeys+1)
	return nil
}

func (bt *BTree) findParent(currentPageID, targetPageID manager.PageID) manager.PageID {
	data, _ := bt.bm.PinPage(currentPageID)
	defer bt.bm.UnpinPage(currentPageID, false)

	nodeType := binary.BigEndian.Uint64(data[0:8])
	if nodeType == leafNode {
		return 0
	}

	numKeys := binary.BigEndian.Uint64(data[8:16])
	for i := 0; i <= int(numKeys); i++ {
		offset := internalHeaderSize + i*(ptrSize+keySize)
		childID := manager.Unsizzle(data[offset : offset+ptrSize])
		if childID == targetPageID {
			return currentPageID
		}
		if parent := bt.findParent(childID, targetPageID); parent != 0 {
			return parent
		}
	}
	return 0
}

func (bt *BTree) insertIntoLeaf(data []byte, key, value uint64) error {
	numKeys := binary.BigEndian.Uint64(data[8:16])
	insertIndex := 0

	for ; insertIndex < int(numKeys); insertIndex++ {
		offset := leafHeaderSize + insertIndex*(keySize+valueSize)
		currentKey := binary.BigEndian.Uint64(data[offset : offset+keySize])
		if key < currentKey {
			break
		} else if key == currentKey {
			return errors.New("duplicate key")
		}
	}

	// Shift entries
	startOffset := leafHeaderSize + insertIndex*(keySize+valueSize)
	bytesToShift := (int(numKeys) - insertIndex) * (keySize + valueSize)
	if bytesToShift > 0 {
		copy(data[startOffset+(keySize+valueSize):], data[startOffset:startOffset+bytesToShift])
	}

	// Insert new key-value
	binary.BigEndian.PutUint64(data[startOffset:], key)
	binary.BigEndian.PutUint64(data[startOffset+keySize:], value)
	binary.BigEndian.PutUint64(data[8:16], numKeys+1)
	return nil
}
