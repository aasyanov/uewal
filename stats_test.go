package uewal

import (
	"sync"
	"testing"
)

func TestStatsCollectorBasic(t *testing.T) {
	var sc statsCollector
	sc.addEvents(10)
	sc.addBatches(2)
	sc.addBytes(500)
	sc.addSynced(500)
	sc.addSync()
	sc.addDrop(3)
	sc.addCorruption()
	sc.storeLSN(42)

	s := sc.snapshot(5, 1024, StateRunning)

	if s.EventsWritten != 10 {
		t.Errorf("EventsWritten=%d, want 10", s.EventsWritten)
	}
	if s.BatchesWritten != 2 {
		t.Errorf("BatchesWritten=%d, want 2", s.BatchesWritten)
	}
	if s.BytesWritten != 500 {
		t.Errorf("BytesWritten=%d, want 500", s.BytesWritten)
	}
	if s.BytesSynced != 500 {
		t.Errorf("BytesSynced=%d, want 500", s.BytesSynced)
	}
	if s.SyncCount != 1 {
		t.Errorf("SyncCount=%d, want 1", s.SyncCount)
	}
	if s.Drops != 3 {
		t.Errorf("Drops=%d, want 3", s.Drops)
	}
	if s.Corruptions != 1 {
		t.Errorf("Corruptions=%d, want 1", s.Corruptions)
	}
	if s.QueueSize != 5 {
		t.Errorf("QueueSize=%d, want 5", s.QueueSize)
	}
	if s.FileSize != 1024 {
		t.Errorf("FileSize=%d, want 1024", s.FileSize)
	}
	if s.LastLSN != 42 {
		t.Errorf("LastLSN=%d, want 42", s.LastLSN)
	}
	if s.State != StateRunning {
		t.Errorf("State=%v, want RUNNING", s.State)
	}
}

func TestStatsCollectorConcurrent(t *testing.T) {
	var sc statsCollector
	const goroutines = 100
	const opsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				sc.addEvents(1)
				sc.addBatches(1)
				sc.addBytes(10)
				sc.addSync()
			}
		}()
	}
	wg.Wait()

	s := sc.snapshot(0, 0, StateRunning)
	expected := uint64(goroutines * opsPerGoroutine)

	if s.EventsWritten != expected {
		t.Errorf("EventsWritten=%d, want %d", s.EventsWritten, expected)
	}
	if s.BatchesWritten != expected {
		t.Errorf("BatchesWritten=%d, want %d", s.BatchesWritten, expected)
	}
	if s.BytesWritten != expected*10 {
		t.Errorf("BytesWritten=%d, want %d", s.BytesWritten, expected*10)
	}
	if s.SyncCount != expected {
		t.Errorf("SyncCount=%d, want %d", s.SyncCount, expected)
	}
}

func TestStatsCollectorLSNConcurrent(t *testing.T) {
	var sc statsCollector
	const goroutines = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			sc.storeLSN(LSN(id))
		}(i)
	}
	wg.Wait()

	lsn := sc.loadLSN()
	if lsn >= uint64(goroutines) {
		t.Errorf("LSN=%d, should be < %d", lsn, goroutines)
	}
}
