package splitordered

import (
	"testing"
)

func TestInsertAndContains(t *testing.T) {
	so := NewSplitOrderedHash()
	if !so.Insert(42) {
		t.Error("Insert failed")
	}
	if !so.Contains(42) {
		t.Error("Contains failed")
	}
	if so.Contains(43) {
		t.Error("Found non-existent key")
	}
}

func TestDelete(t *testing.T) {
	so := NewSplitOrderedHash()
	so.Insert(42)
	if !so.Delete(42) {
		t.Error("Delete failed")
	}
	if so.Contains(42) {
		t.Error("Found deleted key")
	}
}

func TestResize(t *testing.T) {
	so := NewSplitOrderedHash()
	initialSize := so.size
	for i := 0; i < 10; i++ {
		so.Insert(uint64(i))
	}
	if so.size <= initialSize {
		t.Error("Table did not resize")
	}
}

func TestManyItems(t *testing.T) {
	so := NewSplitOrderedHash()
	n := 10000
	// Insert many items
	for i := 0; i < n; i++ {
		if !so.Insert(uint64(i)) {
			t.Fatalf("Insert failed for %d", i)
		}
	}
	// Check all items exist
	for i := 0; i < n; i++ {
		if !so.Contains(uint64(i)) {
			t.Fatalf("Contains failed for %d", i)
		}
	}
	// Check a few non-existent items
	for i := n; i < n+100; i++ {
		if so.Contains(uint64(i)) {
			t.Fatalf("Found non-existent key %d", i)
		}
	}
}

func TestLargeScale(t *testing.T) {
	so := NewSplitOrderedHash()
	const numItems = 100000

	// Insert 100,000 items
	t.Run("Insert 100K items", func(t *testing.T) {
		for i := uint64(0); i < numItems; i++ {
			if !so.Insert(i) {
				t.Errorf("Failed to insert %d", i)
			}
		}
		if so.count != numItems {
			t.Errorf("Expected count %d, got %d", numItems, so.count)
		}
	})

	// Verify all items exist
	t.Run("Verify 100K items", func(t *testing.T) {
		for i := uint64(0); i < numItems; i++ {
			if !so.Find(i) {
				t.Errorf("Failed to find %d", i)
			}
		}
	})

	// Delete all items
	t.Run("Delete 100K items", func(t *testing.T) {
		for i := uint64(0); i < numItems; i++ {
			if !so.Delete(i) {
				t.Errorf("Failed to delete %d", i)
			}
		}
		if so.count != 0 {
			t.Errorf("Expected count 0, got %d", so.count)
		}
	})
}

func TestMillionItems(t *testing.T) {
	so := NewSplitOrderedHash()
	const numItems = 1000000

	// Insert 1 million items
	t.Run("Insert 1M items", func(t *testing.T) {
		for i := uint64(0); i < numItems; i++ {
			if !so.Insert(i) {
				t.Errorf("Failed to insert %d", i)
			}
		}
		if so.count != numItems {
			t.Errorf("Expected count %d, got %d", numItems, so.count)
		}
	})

	// Verify all items exist
	t.Run("Verify 1M items", func(t *testing.T) {
		for i := uint64(0); i < numItems; i++ {
			if !so.Find(i) {
				t.Errorf("Failed to find %d", i)
			}
		}
	})

	// Delete all items
	t.Run("Delete 1M items", func(t *testing.T) {
		for i := uint64(0); i < numItems; i++ {
			if !so.Delete(i) {
				t.Errorf("Failed to delete %d", i)
			}
		}
		if so.count != 0 {
			t.Errorf("Expected count 0, got %d", so.count)
		}
	})
}

func BenchmarkInsert(b *testing.B) {
	so := NewSplitOrderedHash()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		so.Insert(uint64(i))
	}
}

func BenchmarkLargeScale(b *testing.B) {
	so := NewSplitOrderedHash()
	const numItems = 100000

	// Benchmark insertion of 100K items
	b.Run("Insert 100K", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			so = NewSplitOrderedHash()
			for j := uint64(0); j < numItems; j++ {
				so.Insert(j)
			}
		}
	})

	// Benchmark finding 100K items
	b.Run("Find 100K", func(b *testing.B) {
		// Setup: insert items first
		for j := uint64(0); j < numItems; j++ {
			so.Insert(j)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := uint64(0); j < numItems; j++ {
				so.Find(j)
			}
		}
	})

	// Benchmark deletion of 100K items
	b.Run("Delete 100K", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Setup: insert items first
			so = NewSplitOrderedHash()
			for j := uint64(0); j < numItems; j++ {
				so.Insert(j)
			}
			// Benchmark deletion
			for j := uint64(0); j < numItems; j++ {
				so.Delete(j)
			}
		}
	})
}

func BenchmarkMillionItems(b *testing.B) {
	so := NewSplitOrderedHash()
	const numItems = 1000000

	// Benchmark insertion of 1M items
	b.Run("Insert 1M", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			so = NewSplitOrderedHash()
			for j := uint64(0); j < numItems; j++ {
				so.Insert(j)
			}
		}
	})

	// Benchmark finding 1M items
	b.Run("Find 1M", func(b *testing.B) {
		// Setup: insert items first
		for j := uint64(0); j < numItems; j++ {
			so.Insert(j)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := uint64(0); j < numItems; j++ {
				so.Find(j)
			}
		}
	})

	// Benchmark deletion of 1M items
	b.Run("Delete 1M", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Setup: insert items first
			so = NewSplitOrderedHash()
			for j := uint64(0); j < numItems; j++ {
				so.Insert(j)
			}
			// Benchmark deletion
			for j := uint64(0); j < numItems; j++ {
				so.Delete(j)
			}
		}
	})
}
