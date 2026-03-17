package uewal

import (
	"testing"
)

func TestStatsCollector_AddEvents(t *testing.T) {
	sc := &statsCollector{}
	sc.addEvents(10)
	sc.addEvents(5)
	s := sc.snapshot(0, 0, 0, 0, StateInit)
	if s.EventsWritten != 15 {
		t.Errorf("EventsWritten=%d, want 15", s.EventsWritten)
	}
}

func TestStatsCollector_AddBatches(t *testing.T) {
	sc := &statsCollector{}
	sc.addBatches(3)
	sc.addBatches(2)
	s := sc.snapshot(0, 0, 0, 0, StateInit)
	if s.BatchesWritten != 5 {
		t.Errorf("BatchesWritten=%d, want 5", s.BatchesWritten)
	}
}

func TestStatsCollector_AddBytes(t *testing.T) {
	sc := &statsCollector{}
	sc.addBytes(100)
	sc.addBytes(50)
	s := sc.snapshot(0, 0, 0, 0, StateInit)
	if s.BytesWritten != 150 {
		t.Errorf("BytesWritten=%d, want 150", s.BytesWritten)
	}
}

func TestStatsCollector_StoreLSN(t *testing.T) {
	sc := &statsCollector{}
	sc.storeLSN(42)
	if got := sc.loadLSN(); got != 42 {
		t.Errorf("loadLSN()=%d, want 42", got)
	}
	sc.storeLSN(100)
	if got := sc.loadLSN(); got != 100 {
		t.Errorf("loadLSN()=%d, want 100", got)
	}
}

func TestStatsCollector_StoreFirstLSN(t *testing.T) {
	sc := &statsCollector{}
	sc.storeFirstLSN(10)
	s := sc.snapshot(0, 0, 0, 0, StateInit)
	if s.FirstLSN != 10 {
		t.Errorf("FirstLSN=%d, want 10", s.FirstLSN)
	}
	// CompareAndSwap from 0: second call should not overwrite
	sc.storeFirstLSN(20)
	s = sc.snapshot(0, 0, 0, 0, StateInit)
	if s.FirstLSN != 10 {
		t.Errorf("storeFirstLSN(20) should not overwrite; FirstLSN=%d, want 10", s.FirstLSN)
	}
}

func TestStatsCollector_Snapshot(t *testing.T) {
	sc := &statsCollector{}
	sc.addEvents(7)
	sc.addBatches(3)
	sc.addBytes(256)
	sc.storeLSN(100)
	sc.storeFirstLSN(1)
	s := sc.snapshot(5, 1024, 512, 2, StateRunning)

	if s.EventsWritten != 7 {
		t.Errorf("EventsWritten=%d, want 7", s.EventsWritten)
	}
	if s.BatchesWritten != 3 {
		t.Errorf("BatchesWritten=%d, want 3", s.BatchesWritten)
	}
	if s.BytesWritten != 256 {
		t.Errorf("BytesWritten=%d, want 256", s.BytesWritten)
	}
	if s.LastLSN != 100 {
		t.Errorf("LastLSN=%d, want 100", s.LastLSN)
	}
	if s.FirstLSN != 1 {
		t.Errorf("FirstLSN=%d, want 1", s.FirstLSN)
	}
	if s.QueueSize != 5 {
		t.Errorf("QueueSize=%d, want 5", s.QueueSize)
	}
	if s.TotalSize != 1024 {
		t.Errorf("TotalSize=%d, want 1024", s.TotalSize)
	}
	if s.ActiveSegmentSize != 512 {
		t.Errorf("ActiveSegmentSize=%d, want 512", s.ActiveSegmentSize)
	}
	if s.SegmentCount != 2 {
		t.Errorf("SegmentCount=%d, want 2", s.SegmentCount)
	}
	if s.State != StateRunning {
		t.Errorf("State=%v, want RUNNING", s.State)
	}
}

func TestStatsCollector_AddCompressed(t *testing.T) {
	sc := &statsCollector{}
	sc.addCompressed(64)
	sc.addCompressed(32)
	s := sc.snapshot(0, 0, 0, 0, StateInit)
	if s.CompressedBytes != 96 {
		t.Errorf("CompressedBytes=%d, want 96", s.CompressedBytes)
	}
}

func TestStatsCollector_AddCorruption(t *testing.T) {
	sc := &statsCollector{}
	sc.addCorruption()
	sc.addCorruption()
	sc.addCorruption()
	s := sc.snapshot(0, 0, 0, 0, StateInit)
	if s.Corruptions != 3 {
		t.Errorf("Corruptions=%d, want 3", s.Corruptions)
	}
}

func TestStatsCollector_AddImport(t *testing.T) {
	sc := &statsCollector{}
	sc.addImport(2, 512)
	sc.addImport(1, 256)
	s := sc.snapshot(0, 0, 0, 0, StateInit)
	if s.ImportBatches != 3 {
		t.Errorf("ImportBatches=%d, want 3", s.ImportBatches)
	}
	if s.ImportBytes != 768 {
		t.Errorf("ImportBytes=%d, want 768", s.ImportBytes)
	}
}
