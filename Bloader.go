package loader

import (
	"btree"
	"encoding/binary"
	"manager"
	"os"
	"sort"
)

const (
	keySize   = 8
	valueSize = 8
)

type entry struct {
	key   uint64
	value uint64
}

func LoadDataFile(bm *manager.BufferManager, dataFile string) (*btree.BTree, error) {
	// Read and sort all entries first
	entries, err := readAndSortEntries(dataFile)
	if err != nil {
		return nil, err
	}

	// Create B+Tree with bulk loading
	return createBulkLoadedTree(bm, entries)
}

func readAndSortEntries(dataFile string) ([]entry, error) {
	file, err := os.Open(dataFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []entry

	for {
		var key, value uint64
		err := binary.Read(file, binary.BigEndian, &key)
		if err != nil {
			break
		}
		err = binary.Read(file, binary.BigEndian, &value)
		if err != nil {
			break
		}

		entries = append(entries, entry{key, value})
	}

	// Sort entries by key
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})

	return entries, nil
}

func createBulkLoadedTree(bm *manager.BufferManager, entries []entry) (*btree.BTree, error) {
	// Create leaf nodes
	leaves, err := createLeafNodes(bm, entries)
	if err != nil {
		return nil, err
	}

	// Build internal nodes from leaves
	rootID := buildTreeFromLeaves(bm, leaves)

	return &btree.BTree{
		bm:         bm,
		rootPageID: rootID,
	}, nil
}

func createLeafNodes(bm *manager.BufferManager, entries []entry) ([]manager.PageID, error) {
	var leaves []manager.PageID
	var currentLeaf *[manager.PageSize]byte
	var currentLeafID manager.PageID
	entriesPerLeaf := (manager.PageSize - btree.leafHeaderSize) / (keySize + valueSize)

	for i, e := range entries {
		if i%entriesPerLeaf == 0 {
			// Create new leaf node
			if currentLeaf != nil {
				// Finalize previous leaf
				binary.BigEndian.PutUint64(currentLeaf[8:16], uint64(entriesPerLeaf))
			}

			var err error
			currentLeafID, currentLeaf, err = bm.NewPage()
			if err != nil {
				return nil, err
			}
			leaves = append(leaves, currentLeafID)
			btree.InitializeLeafPage(currentLeaf)
		}

		// Calculate offset for this entry
		offset := btree.leafHeaderSize + (i%entriesPerLeaf)*(keySize+valueSize)
		binary.BigEndian.PutUint64(currentLeaf[offset:], e.key)
		binary.BigEndian.PutUint64(currentLeaf[offset+keySize:], e.value)
	}

	// Finalize last leaf
	if currentLeaf != nil {
		finalCount := uint64(len(entries) % entriesPerLeaf)
		if finalCount == 0 {
			finalCount = uint64(entriesPerLeaf)
		}
		binary.BigEndian.PutUint64(currentLeaf[8:16], finalCount)
	}

	return leaves, nil
}

func buildTreeFromLeaves(bm *manager.BufferManager, leaves []manager.PageID) manager.PageID {
	if len(leaves) == 1 {
		return leaves[0]
	}

	var parents []manager.PageID
	pointersPerNode := (manager.PageSize - btree.internalHeaderSize) / (keySize + btree.ptrSize)

	for i := 0; i < len(leaves); i += pointersPerNode {
		end := i + pointersPerNode
		if end > len(leaves) {
			end = len(leaves)
		}

		// Create new internal node
		pageID, data, err := bm.NewPage()
		if err != nil {
			panic(err)
		}
		btree.InitializeInternalPage(data)

		// First pointer
		copy(data[btree.internalHeaderSize:], manager.Sizzle(leaves[i]))

		// Add keys and subsequent pointers
		numKeys := 0
		for j := i + 1; j < end; j++ {
			// Get first key from child node
			childData, _ := bm.PinPage(leaves[j])
			firstKey := binary.BigEndian.Uint64((*childData)[btree.leafHeaderSize:])
			bm.UnpinPage(leaves[j], false)

			offset := btree.internalHeaderSize + numKeys*(btree.ptrSize+keySize) + btree.ptrSize
			binary.BigEndian.PutUint64(data[offset:], firstKey)

			offset += keySize
			copy(data[offset:], manager.Sizzle(leaves[j]))
			numKeys++
		}

		binary.BigEndian.PutUint64(data[8:16], uint64(numKeys))
		parents = append(parents, pageID)
	}

	return buildTreeFromLeaves(bm, parents)
}
