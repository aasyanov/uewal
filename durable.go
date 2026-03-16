package uewal

import (
	"sync"
	"sync/atomic"
)

// durableNotifier implements coalesced fsync notification.
//
// Multiple goroutines call wait(lsn) — all block until fsync covers
// their LSN. The writer goroutine calls advance(lsn) after each fsync.
// One fsync satisfies all waiters whose LSN <= synced LSN.
//
// This is the standard group-commit durability pattern used by
// PostgreSQL, etcd, and CockroachDB.
type durableNotifier struct {
	mu       sync.Mutex
	syncedTo atomic.Uint64
	waiters  []durableWaiter
}

type durableWaiter struct {
	lsn LSN
	ch  chan struct{}
}

// wait blocks until the given LSN has been fsync'd to disk.
// Returns immediately if already synced.
func (d *durableNotifier) wait(lsn LSN) {
	if d.syncedTo.Load() >= lsn {
		return
	}

	ch := make(chan struct{})
	d.mu.Lock()
	if d.syncedTo.Load() >= lsn {
		d.mu.Unlock()
		return
	}
	d.waiters = append(d.waiters, durableWaiter{lsn: lsn, ch: ch})
	d.mu.Unlock()

	<-ch
}

// advance notifies all waiters whose LSN <= syncedLSN.
// Called from the writer goroutine after a successful fsync.
func (d *durableNotifier) advance(syncedLSN LSN) {
	d.syncedTo.Store(syncedLSN)

	d.mu.Lock()
	remaining := d.waiters[:0]
	for _, w := range d.waiters {
		if w.lsn <= syncedLSN {
			close(w.ch)
		} else {
			remaining = append(remaining, w)
		}
	}
	d.waiters = remaining
	d.mu.Unlock()
}

// wakeAll unblocks all remaining waiters (used during shutdown).
func (d *durableNotifier) wakeAll() {
	d.mu.Lock()
	for _, w := range d.waiters {
		close(w.ch)
	}
	d.waiters = nil
	d.mu.Unlock()
}
