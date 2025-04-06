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
	maxLeafEntries     = (manager.PageSize - leafHeaderSize) / (keySize + valueSize)
	maxInternalKeys    = (manager.PageSize - internalHeaderSize - ptrSize) / (keySize + ptrSize)
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

func initializeLeafPage(data *[manager.PageSize]byte) {
	binary.BigEndian.PutUint64(data[0:8], leafNode)
	binary.BigEndian.PutUint64(data[8:16], 0)
	binary.BigEndian.PutUint64(data[16:24], 0)
	binary.BigEndian.PutUint64(data[24:32], 0)
}

func initializeInternalPage(data *[manager.PageSize]byte) {
	binary.BigEndian.PutUint64(data[0:8], internalNode)
	binary.BigEndian.PutUint64(data[8:16], 0)
}

func (bt *BTree) Get(key uint64) (uint64, error) {
	return bt.search(bt.rootPageID, key)
}

func (bt *BTree) search(pageID manager.PageID, key uint64) (uint64, error) {
	data, err := bt.bm.PinPage(pageID)
	if err != nil {
		return 0, err
	}
	defer bt.bm.UnpinPage(pageID, false)

	nodeType := binary.BigEndian.Uint64(data[0:8])
	if nodeType == leafNode {
		return bt.searchLeaf(data, key)
	}
	return bt.searchInternal(data, key)
}

func (bt *BTree) searchLeaf(data *[manager.PageSize]byte, key uint64) (uint64, error) {
	numKeys := binary.BigEndian.Uint64(data[8:16])
	low := 0
	high := int(numKeys) - 1

	for low <= high {
		mid := (low + high) / 2
		offset := leafHeaderSize + mid*(keySize+valueSize)
		currentKey := binary.BigEndian.Uint64(data[offset:])

		switch {
		case key == currentKey:
			return binary.BigEndian.Uint64(data[offset+keySize:]), nil
		case key < currentKey:
			high = mid - 1
		default:
			low = mid + 1
		}
	}
	return 0, errors.New("key not found")
}

func (bt *BTree) searchInternal(data *[manager.PageSize]byte, key uint64) (uint64, error) {
	numKeys := binary.BigEndian.Uint64(data[8:16])
	low := 0
	high := int(numKeys) - 1
	childIndex := 0

	for low <= high {
		mid := (low + high) / 2
		keyOffset := internalHeaderSize + mid*(ptrSize+keySize) + ptrSize
		currentKey := binary.BigEndian.Uint64(data[keyOffset:])

		if key < currentKey {
			high = mid - 1
			childIndex = mid
		} else {
			low = mid + 1
			childIndex = mid + 1
		}
	}

	ptrOffset := internalHeaderSize + childIndex*(ptrSize+keySize)
	childID := manager.Unsizzle([8]byte(data[ptrOffset:]))
	return bt.search(childID, key)
}

// Updated Insert implementation with full split propagation
func (bt *BTree) Insert(key, value uint64) error {
	splitKey, newChild, err := bt.insert(bt.rootPageID, key, value)
	if err != nil {
		return err
	}

	// Handle root split
	if newChild != 0 {
		newRootID, rootData, _ := bt.bm.NewPage()
		initializeInternalPage(rootData)

		// Set first pointer to old root
		copy(rootData[internalHeaderSize:], manager.Sizzle(bt.rootPageID))
		// Set split key
		binary.BigEndian.PutUint64(rootData[internalHeaderSize+ptrSize:], splitKey)
		// Set second pointer to new child
		copy(rootData[internalHeaderSize+ptrSize+keySize:], manager.Sizzle(newChild))

		binary.BigEndian.PutUint64(rootData[8:16], 1) // numKeys = 1
		bt.rootPageID = newRootID
	}
	return nil
}

func (bt *BTree) insert(pageID manager.PageID, key, value uint64) (uint64, manager.PageID, error) {
	data, err := bt.bm.PinPage(pageID)
	if err != nil {
		return 0, 0, err
	}
	defer bt.bm.UnpinPage(pageID, true)

	nodeType := binary.BigEndian.Uint64(data[0:8])
	if nodeType == leafNode {
		return bt.insertLeaf(data, pageID, key, value)
	}
	return bt.insertInternal(data, pageID, key, value)
}

func (bt *BTree) insertLeaf(data *[manager.PageSize]byte, pageID manager.PageID, key, value uint64) (uint64, manager.PageID, error) {
	numKeys := binary.BigEndian.Uint64(data[8:16])
	insertPos := bt.findLeafInsertPosition(data, numKeys, key)

	// Update existing key if found
	if insertPos < numKeys {
		offset := leafHeaderSize + insertPos*(keySize+valueSize)
		currentKey := binary.BigEndian.Uint64(data[offset:])
		if currentKey == key {
			binary.BigEndian.PutUint64(data[offset+keySize:], value)
			return 0, 0, nil // No split needed
		}
	}

	if numKeys < maxLeafEntries {
		bt.insertLeafEntry(data, numKeys, insertPos, key, value)
		return 0, 0, nil
	}

	// Split required
	newPageID, newData, _ := bt.bm.NewPage()
	initializeLeafPage(newData)
	splitPos := numKeys / 2
	splitKey := binary.BigEndian.Uint64(data[leafHeaderSize+splitPos*(keySize+valueSize):])

	// Split entries
	bt.splitLeaf(data, newData, splitPos)

	// Insert into appropriate node
	if insertPos > splitPos {
		bt.insertLeafEntry(newData, numKeys-splitPos, insertPos-splitPos, key, value)
	} else {
		bt.insertLeafEntry(data, splitPos, insertPos, key, value)
	}

	// Maintain linked list
	nextPage := binary.BigEndian.Uint64(data[16:24])
	binary.BigEndian.PutUint64(newData[16:24], nextPage)
	binary.BigEndian.PutUint64(newData[24:32], uint64(pageID))
	binary.BigEndian.PutUint64(data[16:24], uint64(newPageID))

	return splitKey, newPageID, nil
}

func (bt *BTree) insertInternal(data *[manager.PageSize]byte, pageID manager.PageID, key, value uint64) (uint64, manager.PageID, error) {
	numKeys := binary.BigEndian.Uint64(data[8:16])
	insertPos := bt.findInternalInsertPosition(data, numKeys, key)

	// Recurse to child
	childOffset := internalHeaderSize + insertPos*(ptrSize+keySize)
	childID := manager.Unsizzle([8]byte(data[childOffset:]))

	promotedKey, newChild, err := bt.insert(childID, key, value)
	if err != nil {
		return 0, 0, err
	}

	if newChild == 0 {
		return 0, 0, nil // No propagation needed
	}

	// Insert new key and pointer in internal node
	if numKeys < maxInternalKeys {
		bt.insertInternalEntry(data, numKeys, insertPos, promotedKey, newChild)
		return 0, 0, nil
	}

	// Split internal node
	newPageID, newData, _ := bt.bm.NewPage()
	initializeInternalPage(newData)
	splitPos := numKeys / 2
	promotedSplitKey := bt.splitInternal(data, newData, splitPos)

	// Determine where to insert
	if insertPos > splitPos {
		bt.insertInternalEntry(newData, numKeys-splitPos-1, insertPos-splitPos-1, promotedKey, newChild)
	} else {
		bt.insertInternalEntry(data, splitPos, insertPos, promotedKey, newChild)
	}

	return promotedSplitKey, newPageID, nil
}

//

func (bt *BTree) findLeafInsertPosition(data *[manager.PageSize]byte, numKeys uint64, key uint64) uint64 {
	low := 0
	high := int(numKeys) - 1
	var mid int

	for low <= high {
		mid = (low + high) / 2
		offset := leafHeaderSize + mid*(keySize+valueSize)
		currentKey := binary.BigEndian.Uint64(data[offset:])

		switch {
		case key == currentKey:
			return uint64(mid)
		case key < currentKey:
			high = mid - 1
		default:
			low = mid + 1
		}
	}
	return uint64(low)
}

func (bt *BTree) insertLeafEntry(data *[manager.PageSize]byte, numKeys, pos uint64, key, value uint64) error {
	startOffset := leafHeaderSize + pos*(keySize+valueSize)
	endOffset := leafHeaderSize + numKeys*(keySize+valueSize)
	copy(data[startOffset+keySize+valueSize:], data[startOffset:endOffset])

	binary.BigEndian.PutUint64(data[startOffset:], key)
	binary.BigEndian.PutUint64(data[startOffset+keySize:], value)
	binary.BigEndian.PutUint64(data[8:16], numKeys+1)
	return nil
}

func (bt *BTree) splitLeaf(oldData, newData *[manager.PageSize]byte, splitPos uint64) {
	copy(newData[leafHeaderSize:], oldData[leafHeaderSize+splitPos*(keySize+valueSize):])

	oldNumKeys := binary.BigEndian.Uint64(oldData[8:16])
	binary.BigEndian.PutUint64(oldData[8:16], splitPos)
	binary.BigEndian.PutUint64(newData[8:16], oldNumKeys-splitPos)

	// Update linked list
	nextPage := binary.BigEndian.Uint64(oldData[16:24])
	binary.BigEndian.PutUint64(newData[16:24], nextPage)
	binary.BigEndian.PutUint64(newData[24:32], binary.BigEndian.Uint64(oldData[24:32]))
	binary.BigEndian.PutUint64(oldData[16:24], binary.BigEndian.Uint64(newData[24:32]))
}

func (bt *BTree) insertInternal(data *[manager.PageSize]byte, pageID manager.PageID, key uint64, value uint64, newChildID manager.PageID) error {
	numKeys := binary.BigEndian.Uint64(data[8:16])
	insertPos := bt.findInternalInsertPosition(data, numKeys, key)

	if numKeys < maxInternalKeys {
		return bt.insertInternalEntry(data, numKeys, insertPos, key, newChildID)
	}

	newPageID, newData, _ := bt.bm.NewPage()
	initializeInternalPage(newData)
	splitPos := numKeys / 2
	promotedKey := bt.splitInternal(data, newData, splitPos)

	if insertPos > splitPos {
		return bt.insertInternalEntry(newData, numKeys-splitPos-1, insertPos-splitPos-1, key, newChildID)
	}
	return bt.insertInternalEntry(data, splitPos, insertPos, key, newChildID)
}

func (bt *BTree) findInternalInsertPosition(data *[manager.PageSize]byte, numKeys uint64, key uint64) uint64 {
	low := 0
	high := int(numKeys) - 1
	var pos int

	for low <= high {
		mid := (low + high) / 2
		keyOffset := internalHeaderSize + mid*(ptrSize+keySize) + ptrSize
		currentKey := binary.BigEndian.Uint64(data[keyOffset:])

		switch {
		case key < currentKey:
			high = mid - 1
			pos = mid
		default:
			low = mid + 1
			pos = mid + 1
		}
	}
	return uint64(pos)
}

func (bt *BTree) insertInternalEntry(data *[manager.PageSize]byte, numKeys, pos uint64, key uint64, childID manager.PageID) error {
	startOffset := internalHeaderSize + pos*(ptrSize+keySize)
	endOffset := internalHeaderSize + numKeys*(ptrSize+keySize)
	copy(data[startOffset+ptrSize+keySize:], data[startOffset:endOffset])

	copy(data[startOffset:], manager.Sizzle(childID))
	binary.BigEndian.PutUint64(data[startOffset+ptrSize:], key)
	binary.BigEndian.PutUint64(data[8:16], numKeys+1)
	return nil
}

func (bt *BTree) splitInternal(oldData, newData *[manager.PageSize]byte, splitPos uint64) uint64 {
	promotedKey := binary.BigEndian.Uint64(oldData[internalHeaderSize+splitPos*(ptrSize+keySize)+ptrSize:])

	// Copy right entries
	startOffset := internalHeaderSize + (splitPos+1)*(ptrSize+keySize)
	copy(newData[internalHeaderSize:], oldData[startOffset:])

	// Update counts
	oldNumKeys := binary.BigEndian.Uint64(oldData[8:16])
	binary.BigEndian.PutUint64(oldData[8:16], splitPos)
	binary.BigEndian.PutUint64(newData[8:16], oldNumKeys-splitPos-1)

	return promotedKey
}

func (bt *BTree) findParent(currentPageID, targetPageID manager.PageID) manager.PageID {
	data, _ := bt.bm.PinPage(currentPageID)
	defer bt.bm.UnpinPage(currentPageID, false)

	nodeType := binary.BigEndian.Uint64(data[0:8])
	if nodeType == leafNode {
		return 0
	}

	numKeys := binary.BigEndian.Uint64(data[8:16])
	for i := uint64(0); i <= numKeys; i++ {
		offset := internalHeaderSize + i*(ptrSize+keySize)
		childID := manager.Unsizzle([8]byte(data[offset:]))
		if childID == targetPageID {
			return currentPageID
		}
		if parent := bt.findParent(childID, targetPageID); parent != 0 {
			return parent
		}
	}
	return 0
}

func (bt *BTree) createNewRoot(leftChild, rightChild manager.PageID, key uint64) error {
	newRootID, rootData, _ := bt.bm.NewPage()
	initializeInternalPage(rootData)

	binary.BigEndian.PutUint64(rootData[8:16], 1)
	copy(rootData[internalHeaderSize:], manager.Sizzle(leftChild))
	binary.BigEndian.PutUint64(rootData[internalHeaderSize+ptrSize:], key)
	copy(rootData[internalHeaderSize+ptrSize+keySize:], manager.Sizzle(rightChild))

	bt.rootPageID = newRootID
	return nil
}

func Sizzle(pageID manager.PageID) [8]byte {
	return manager.Sizzle(pageID)
}

func Unsizzle(buf [8]byte) manager.PageID {
	return manager.Unsizzle(buf)
}
