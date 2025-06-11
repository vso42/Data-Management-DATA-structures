package splitordered

import (
	"math/bits"
)

const (
	ehSegmentBits = 8
	ehSegmentSize = 1 << ehSegmentBits
	ehMaxSegments = 1024
	maxBucketSize = 4 // Maximum number of items per bucket
)

type Bucket struct {
	items      []uint64
	localDepth uint8
}

type ehSegment [ehSegmentSize]*Bucket

type ExtensibleHash struct {
	segments [ehMaxSegments]*ehSegment
	size     uint64
	count    uint64
}

func NewExtensibleHash() *ExtensibleHash {
	eh := &ExtensibleHash{size: 2}
	seg := &ehSegment{}
	seg[0] = &Bucket{
		items:      make([]uint64, 0, maxBucketSize),
		localDepth: 0,
	}
	eh.segments[0] = seg
	return eh
}

func (eh *ExtensibleHash) hash(key uint64) uint64 {
	return key
}

func (eh *ExtensibleHash) getBucketIndex(key uint64) uint64 {
	return key % eh.size
}

func (eh *ExtensibleHash) getBucket(bucketIndex uint64) (*ehSegment, *Bucket) {
	idx := bucketIndex / ehSegmentSize
	seg := eh.segments[idx]
	if seg == nil {
		return nil, nil
	}
	return seg, seg[bucketIndex%ehSegmentSize]
}

func (eh *ExtensibleHash) setBucket(bucketIndex uint64, bucket *Bucket) {
	idx := bucketIndex / ehSegmentSize
	seg := eh.segments[idx]
	if seg == nil {
		seg = &ehSegment{}
		eh.segments[idx] = seg
	}
	seg[bucketIndex%ehSegmentSize] = bucket
}

func (eh *ExtensibleHash) Insert(key uint64) bool {
	bucketIndex := eh.getBucketIndex(key)
	_, bucket := eh.getBucket(bucketIndex)
	if bucket == nil {
		eh.initializeBucket(bucketIndex, eh.size)
		_, bucket = eh.getBucket(bucketIndex)
		if bucket == nil {
			return false
		}
	}

	// Check if key already exists
	for _, item := range bucket.items {
		if item == key {
			return false
		}
	}

	// If bucket is full, we need to split
	if len(bucket.items) >= maxBucketSize {
		if bucket.localDepth == eh.globalDepth() {
			eh.doubleSize()
		}
		eh.splitBucket(bucketIndex)
		// Recalculate bucket index as size might have changed
		bucketIndex = eh.getBucketIndex(key)
		_, bucket = eh.getBucket(bucketIndex)
	}

	bucket.items = append(bucket.items, key)
	eh.count++
	return true
}

func (eh *ExtensibleHash) Find(key uint64) bool {
	bucketIndex := eh.getBucketIndex(key)
	_, bucket := eh.getBucket(bucketIndex)
	if bucket == nil {
		return false
	}

	for _, item := range bucket.items {
		if item == key {
			return true
		}
	}
	return false
}

func (eh *ExtensibleHash) Delete(key uint64) bool {
	bucketIndex := eh.getBucketIndex(key)
	_, bucket := eh.getBucket(bucketIndex)
	if bucket == nil {
		return false
	}

	for i, item := range bucket.items {
		if item == key {
			// Remove item by swapping with last element and truncating
			bucket.items[i] = bucket.items[len(bucket.items)-1]
			bucket.items = bucket.items[:len(bucket.items)-1]
			eh.count--
			return true
		}
	}
	return false
}

func (eh *ExtensibleHash) globalDepth() uint8 {
	return uint8(63 - bits.LeadingZeros64(eh.size))
}

func (eh *ExtensibleHash) doubleSize() {
	oldSize := eh.size
	eh.size *= 2
	// Initialize all new buckets
	for i := oldSize; i < eh.size; i++ {
		eh.initializeBucket(i, eh.size)
	}
}

func (eh *ExtensibleHash) initializeBucket(bucketIndex, size uint64) {
	if bucketIndex >= size {
		return
	}
	parent := ehGetParent(bucketIndex)
	if parent >= size {
		return
	}

	_, parentBucket := eh.getBucket(parent)
	if parentBucket == nil {
		eh.initializeBucket(parent, size)
		_, parentBucket = eh.getBucket(parent)
		if parentBucket == nil {
			return
		}
	}

	newBucket := &Bucket{
		items:      make([]uint64, 0, maxBucketSize),
		localDepth: parentBucket.localDepth,
	}
	eh.setBucket(bucketIndex, newBucket)
}

func ehGetParent(bucketIndex uint64) uint64 {
	mask := ^(uint64(1) << (63 - bits.LeadingZeros64(bucketIndex)))
	return bucketIndex & mask
}

func (eh *ExtensibleHash) splitBucket(bucketIndex uint64) {
	_, bucket := eh.getBucket(bucketIndex)
	if bucket == nil {
		return
	}
	bucket.localDepth++

	// Create new bucket
	newBucket := &Bucket{
		items:      make([]uint64, 0, maxBucketSize),
		localDepth: bucket.localDepth,
	}

	// Redistribute items
	var itemsToKeep []uint64
	for _, item := range bucket.items {
		if (eh.hash(item)>>(bucket.localDepth-1))&1 == 1 {
			newBucket.items = append(newBucket.items, item)
		} else {
			itemsToKeep = append(itemsToKeep, item)
		}
	}
	bucket.items = itemsToKeep

	// Update directory entries
	dirSize := eh.size
	for i := uint64(0); i < dirSize; i++ {
		_, currBucket := eh.getBucket(i)
		if currBucket == bucket {
			if (i>>(bucket.localDepth-1))&1 == 1 {
				eh.setBucket(i, newBucket)
			}
		}
	}
}

func (eh *ExtensibleHash) Count() uint64 {
	return eh.count
}
