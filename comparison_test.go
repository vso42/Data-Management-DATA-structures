package splitordered

import (
	"testing"
)

func BenchmarkComparison(b *testing.B) {
	const numItems = 100000

	// Benchmark Split-Ordered List
	b.Run("SplitOrdered-100K", func(b *testing.B) {
		so := NewSplitOrderedHash()
		b.Run("Insert", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				so = NewSplitOrderedHash()
				for j := uint64(0); j < numItems; j++ {
					so.Insert(j)
				}
			}
		})

		b.Run("Find", func(b *testing.B) {
			// Setup
			so = NewSplitOrderedHash()
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

		b.Run("Delete", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Setup
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
	})

	// Benchmark Extensible Hash
	b.Run("ExtensibleHash-100K", func(b *testing.B) {
		eh := NewExtensibleHash()
		b.Run("Insert", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				eh = NewExtensibleHash()
				for j := uint64(0); j < numItems; j++ {
					eh.Insert(j)
				}
			}
		})

		b.Run("Find", func(b *testing.B) {
			// Setup
			eh = NewExtensibleHash()
			for j := uint64(0); j < numItems; j++ {
				eh.Insert(j)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := uint64(0); j < numItems; j++ {
					eh.Find(j)
				}
			}
		})

		b.Run("Delete", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Setup
				eh = NewExtensibleHash()
				for j := uint64(0); j < numItems; j++ {
					eh.Insert(j)
				}
				// Benchmark deletion
				for j := uint64(0); j < numItems; j++ {
					eh.Delete(j)
				}
			}
		})
	})
}

func BenchmarkComparison1M(b *testing.B) {
	const numItems = 1000000

	// Benchmark Split-Ordered List
	b.Run("SplitOrdered-1M", func(b *testing.B) {
		so := NewSplitOrderedHash()
		b.Run("Insert", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				so = NewSplitOrderedHash()
				for j := uint64(0); j < numItems; j++ {
					so.Insert(j)
				}
			}
		})

		b.Run("Find", func(b *testing.B) {
			// Setup
			so = NewSplitOrderedHash()
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

		b.Run("Delete", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Setup
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
	})

	// Benchmark Extensible Hash
	b.Run("ExtensibleHash-1M", func(b *testing.B) {
		eh := NewExtensibleHash()
		b.Run("Insert", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				eh = NewExtensibleHash()
				for j := uint64(0); j < numItems; j++ {
					eh.Insert(j)
				}
			}
		})

		b.Run("Find", func(b *testing.B) {
			// Setup
			eh = NewExtensibleHash()
			for j := uint64(0); j < numItems; j++ {
				eh.Insert(j)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := uint64(0); j < numItems; j++ {
					eh.Find(j)
				}
			}
		})

		b.Run("Delete", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Setup
				eh = NewExtensibleHash()
				for j := uint64(0); j < numItems; j++ {
					eh.Insert(j)
				}
				// Benchmark deletion
				for j := uint64(0); j < numItems; j++ {
					eh.Delete(j)
				}
			}
		})
	})
}

func BenchmarkComparison1K(b *testing.B) {
	const numItems = 1000

	// Benchmark Split-Ordered List
	b.Run("SplitOrdered-1K", func(b *testing.B) {
		so := NewSplitOrderedHash()
		b.Run("Insert", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				so = NewSplitOrderedHash()
				for j := uint64(0); j < numItems; j++ {
					so.Insert(j)
				}
			}
		})

		b.Run("Find", func(b *testing.B) {
			// Setup
			so = NewSplitOrderedHash()
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

		b.Run("Delete", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Setup
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
	})

	// Benchmark Extensible Hash
	b.Run("ExtensibleHash-1K", func(b *testing.B) {
		eh := NewExtensibleHash()
		b.Run("Insert", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				eh = NewExtensibleHash()
				for j := uint64(0); j < numItems; j++ {
					eh.Insert(j)
				}
			}
		})

		b.Run("Find", func(b *testing.B) {
			// Setup
			eh = NewExtensibleHash()
			for j := uint64(0); j < numItems; j++ {
				eh.Insert(j)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := uint64(0); j < numItems; j++ {
					eh.Find(j)
				}
			}
		})

		b.Run("Delete", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Setup
				eh = NewExtensibleHash()
				for j := uint64(0); j < numItems; j++ {
					eh.Insert(j)
				}
				// Benchmark deletion
				for j := uint64(0); j < numItems; j++ {
					eh.Delete(j)
				}
			}
		})
	})
}

// Benchmark for 100 items for quick comparison
func BenchmarkComparison100(b *testing.B) {
	const numItems = 100

	// Benchmark Split-Ordered List
	b.Run("SplitOrdered-100", func(b *testing.B) {
		so := NewSplitOrderedHash()
		b.Run("Insert", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				so = NewSplitOrderedHash()
				for j := uint64(0); j < numItems; j++ {
					so.Insert(j)
				}
			}
		})

		b.Run("Find", func(b *testing.B) {
			// Setup
			so = NewSplitOrderedHash()
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

		b.Run("Delete", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Setup
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
	})

	// Benchmark Extensible Hash
	b.Run("ExtensibleHash-100", func(b *testing.B) {
		eh := NewExtensibleHash()
		b.Run("Insert", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				eh = NewExtensibleHash()
				for j := uint64(0); j < numItems; j++ {
					eh.Insert(j)
				}
			}
		})

		b.Run("Find", func(b *testing.B) {
			// Setup
			eh = NewExtensibleHash()
			for j := uint64(0); j < numItems; j++ {
				eh.Insert(j)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := uint64(0); j < numItems; j++ {
					eh.Find(j)
				}
			}
		})

		b.Run("Delete", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Setup
				eh = NewExtensibleHash()
				for j := uint64(0); j < numItems; j++ {
					eh.Insert(j)
				}
				// Benchmark deletion
				for j := uint64(0); j < numItems; j++ {
					eh.Delete(j)
				}
			}
		})
	})
}
