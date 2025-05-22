package splitordered

import (
	"math/bits"
)

const (
	segmentBits   = 8
	segmentSize   = 1 << segmentBits
	maxSegments   = 1024
	maxLoadFactor = 4
)

type node struct {
	key  uint64
	next *node
}

type segment [segmentSize]*node

type SplitOrderedHash struct {
	segments [maxSegments]*segment
	size     uint64
	count    uint64
}

// NewSplitOrderedHash creates an empty hash with initial size 2.
func NewSplitOrderedHash() *SplitOrderedHash {
	so := &SplitOrderedHash{size: 2}
	seg := &segment{}
	seg[0] = &node{key: so_dummykey(0)}
	so.segments[0] = seg
	return so
}

// Insert adds key if absent, returns true on success.
func (so *SplitOrderedHash) Insert(key uint64) bool {
	soKey := so_regularkey(key)
	newNode := &node{key: soKey}

	for {
		sz := so.size
		bucket := key % sz
		_, dummy := so.getBucket(bucket)
		if dummy == nil {
			so.initializeBucket(bucket, sz)
			continue
		}

		if listInsert(&dummy.next, newNode) {
			so.count++
			maxSize := segmentSize * maxSegments
			if so.count/sz > maxLoadFactor && sz < uint64(maxSize) {
				so.size = sz * 2
			}
			return true
		}
		return false
	}
}

// Delete removes key if present, returns true on success.
func (so *SplitOrderedHash) Delete(key uint64) bool {
	soKey := so_regularkey(key)
	sz := so.size
	bucket := key % sz
	_, dummy := so.getBucket(bucket)
	if dummy == nil {
		return false
	}

	if listDelete(&dummy.next, soKey) {
		so.count--
		return true
	}
	return false
}

func so_regularkey(key uint64) uint64 {
	return reverseBits(key | (1 << 63))
}

func so_dummykey(key uint64) uint64 {
	return reverseBits(key)
}

func reverseBits(x uint64) uint64 {
	return bits.Reverse64(x)
}

func (so *SplitOrderedHash) getBucket(bucket uint64) (*segment, *node) {
	idx := bucket / segmentSize
	seg := so.segments[idx]
	if seg == nil {
		return nil, nil
	}
	nodePtr := seg[bucket%segmentSize]
	return seg, nodePtr
}

func (so *SplitOrderedHash) initializeBucket(bucket, size uint64) {
	maxSize := segmentSize * maxSegments
	if bucket >= uint64(maxSize) {
		return
	}
	parent := getParent(bucket)
	if parent >= size {
		return
	}

	_, pd := so.getBucket(parent)
	if pd == nil {
		so.initializeBucket(parent, size)
		_, pd = so.getBucket(parent)
		if pd == nil {
			return
		}
	}
	dummyKey := so_dummykey(bucket)
	newDummy := &node{key: dummyKey}
	if listInsert(&pd.next, newDummy) {
		so.setBucket(bucket, newDummy)
	} else {
		curr := pd.next
		for curr != nil && curr.key < dummyKey {
			curr = curr.next
		}
		if curr != nil && curr.key == dummyKey {
			so.setBucket(bucket, curr)
		}
	}
}

func getParent(bucket uint64) uint64 {
	mask := ^(uint64(1) << (63 - bits.LeadingZeros64(bucket)))
	return bucket & mask
}

func (so *SplitOrderedHash) setBucket(bucket uint64, n *node) {
	idx := bucket / segmentSize
	seg := so.segments[idx]
	if seg == nil {
		seg = &segment{}
		so.segments[idx] = seg
	}
	seg[bucket%segmentSize] = n
}

// List operations (single-threaded)
func listInsert(head **node, newNode *node) bool {
	prev := head
	curr := *prev
	for curr != nil {
		if curr.key < newNode.key {
			prev = &curr.next
			curr = curr.next
		} else {
			break
		}
	}
	if curr != nil && curr.key == newNode.key {
		return false
	}
	newNode.next = curr
	*prev = newNode
	return true
}

func listDelete(head **node, key uint64) bool {
	prev := head
	curr := *prev
	for curr != nil && curr.key < key {
		prev = &curr.next
		curr = curr.next
	}
	if curr == nil || curr.key != key {
		return false
	}
	*prev = curr.next
	return true
}

// Contains returns true if key exists.
func (so *SplitOrderedHash) Contains(key uint64) bool {
	soKey := so_regularkey(key)
	_, dummy := so.getBucket(key % so.size)
	if dummy == nil {
		return false
	}
	curr := dummy.next
	for curr != nil && curr.key < soKey {
		curr = curr.next
	}
	return curr != nil && curr.key == soKey
}

// Find checks if a key exists in the hash table
func (so *SplitOrderedHash) Find(key uint64) bool {
	soKey := so_regularkey(key)
	bucket := key % so.size
	_, dummy := so.getBucket(bucket)
	if dummy == nil {
		return false
	}
	curr := dummy.next
	for curr != nil && curr.key < soKey {
		curr = curr.next
	}
	return curr != nil && curr.key == soKey
}
