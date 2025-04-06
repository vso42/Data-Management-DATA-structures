package loader

import (
	"btree"
	"encoding/binary"
	"manager"
	"os"
)

func LoadDataFile(bm *manager.BufferManager, dataFile string) (*btree.BTree, error) {
	bt := btree.NewBTree(bm)

	file, err := os.Open(dataFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	for {
		var key, value uint64
		if err := binary.Read(file, binary.BigEndian, &key); err != nil {
			break
		}
		if err := binary.Read(file, binary.BigEndian, &value); err != nil {
			break
		}

		if err := bt.Insert(key, value); err != nil {
			return nil, err
		}
	}

	return bt, nil
}
