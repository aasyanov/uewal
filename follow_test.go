package uewal

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestFollow_Basic(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		w.Append([]byte("existing"))
	}
	w.Flush()

	it, err := w.Follow(0)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	received := make(chan int, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for it.Next() {
			count++
			if count >= 8 {
				break
			}
		}
		received <- count
	}()

	time.Sleep(30 * time.Millisecond)

	for i := 0; i < 3; i++ {
		w.Append([]byte("new"))
	}
	w.Flush()

	select {
	case count := <-received:
		if count != 8 {
			t.Fatalf("expected 8 events (5 existing + 3 new), got %d", count)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Follow timed out")
	}

	it.Close()
	wg.Wait()
	w.Shutdown(context.Background())
}

func TestFollow_Close(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	it, err := w.Follow(0)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		for it.Next() {
		}
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	it.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not unblock Follow")
	}

	w.Shutdown(context.Background())
}

func TestFollow_FromLSN(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		w.Append([]byte("data"))
	}
	w.Flush()

	it, err := w.Follow(8)
	if err != nil {
		t.Fatal(err)
	}

	received := make(chan int, 1)
	go func() {
		count := 0
		for it.Next() {
			count++
			if count >= 3 {
				break
			}
		}
		received <- count
	}()

	select {
	case count := <-received:
		if count != 3 {
			t.Fatalf("from LSN 8: expected 3 events (8,9,10), got %d", count)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Follow timed out")
	}

	it.Close()
	w.Shutdown(context.Background())
}

func TestFollow_NewData(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	it, err := w.Follow(0)
	if err != nil {
		t.Fatal(err)
	}

	received := make(chan LSN, 10)
	go func() {
		for it.Next() {
			received <- it.Event().LSN
			if it.Event().LSN >= 3 {
				break
			}
		}
	}()

	for i := 0; i < 3; i++ {
		w.Append([]byte("streaming"))
		w.Flush()
		time.Sleep(10 * time.Millisecond)
	}

	timeout := time.After(5 * time.Second)
	for i := 0; i < 3; i++ {
		select {
		case lsn := <-received:
			if lsn != LSN(i+1) {
				t.Fatalf("event %d: LSN %d, want %d", i, lsn, i+1)
			}
		case <-timeout:
			t.Fatalf("timed out waiting for event %d", i)
		}
	}

	it.Close()
	w.Shutdown(context.Background())
}
