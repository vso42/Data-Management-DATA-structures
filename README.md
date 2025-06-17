# Data Structures Implementation

This repository contains implementations of two important data structures: a B-tree and a Split-Ordered List (also known as a Split-Ordered Hash Table). Both implementations are written in Go and are designed for high performance and reliability.

## B-tree Implementation

The B-tree implementation provides a disk-based B-tree data structure that supports efficient key-value storage and retrieval operations.


### Key Components
- `BtreeInterface.go`: Main interface and implementation of the B-tree operations
- `Bloader.go`: Buffer management and page loading functionality
- `Bmanager.go`: Buffer manager implementation for disk I/O operations

### Usage
```go
// Create a new B-tree instance
bm := manager.NewBufferManager()
btree := btree.NewBTree(bm)

// Insert key-value pairs
btree.Insert(key, value)

// Search for a value
value, err := btree.Get(key)
```

## Split-Ordered List Implementation

The Split-Ordered List is a concurrent hash table implementation that provides efficient insert, delete, and search operations with good scalability.


### Key Components
- `splitordered.go`: Core implementation of the Split-Ordered List
- `extensible_hash.go`: Extensible hashing implementation
- `comparison_test.go`: Performance comparison tests
- `splitordered_test.go`: Unit tests for the implementation

### Usage
```go
// Create a new Split-Ordered List
so := splitordered.NewSplitOrderedHash()

// Insert a key
success := so.Insert(key)

// Check if a key exists
exists := so.Contains(key)

// Delete a key
deleted := so.Delete(key)
```



