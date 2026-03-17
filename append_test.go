package uewal

import (
	"sync"
	"testing"
)

func TestLSNCounter_StoreLoad(t *testing.T) {
	c := &lsnCounter{}
	if c.current() != 0 {
		t.Fatalf("initial current: got %d, want 0", c.current())
	}
	c.store(42)
	if c.current() != 42 {
		t.Fatalf("after store(42): got %d, want 42", c.current())
	}
	c.store(100)
	if c.current() != 100 {
		t.Fatalf("after store(100): got %d, want 100", c.current())
	}
}

func TestLSNCounter_Concurrent(t *testing.T) {
	c := &lsnCounter{}
	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			c.val.Add(1)
			wg.Done()
		}()
	}
	wg.Wait()
	if got := c.current(); got != n {
		t.Fatalf("got %d, want %d", got, n)
	}
}

func TestRecordSlicePool_GetPut(t *testing.T) {
	s, sp := getRecordSlice(3)
	if len(s) != 3 {
		t.Fatalf("len(s)=%d, want 3", len(s))
	}
	if sp == nil {
		t.Fatal("sp is nil")
	}
	putRecordSlice(sp, s)
}

func TestRecordSlicePool_Reuse(t *testing.T) {
	s1, sp1 := getRecordSlice(1)
	cap1 := cap(s1)
	putRecordSlice(sp1, s1)

	s2, sp2 := getRecordSlice(1)
	if cap(s2) < cap1 {
		t.Fatalf("expected reuse: cap(s2)=%d < cap1=%d", cap(s2), cap1)
	}
	putRecordSlice(sp2, s2)
}
