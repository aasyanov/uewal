package uewal

import (
	"sync"
	"sync/atomic"
	"time"
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
	for {
		cur := d.syncedTo.Load()
		if syncedLSN <= cur {
			return
		}
		if d.syncedTo.CompareAndSwap(cur, syncedLSN) {
			break
		}
	}

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

// durableSync performs a mutex-protected fsync + advance.
// Multiple concurrent callers coalesce: the first does the fsync,
// subsequent callers find syncedTo already advanced and skip.
func (w *WAL) durableSync() error {
	w.durableMu.Lock()
	defer w.durableMu.Unlock()

	currentLSN := w.lsn.current()
	if w.durable.syncedTo.Load() >= currentLSN {
		return nil
	}
	active := w.mgr.active()
	if active.storage != nil {
		if err := active.storage.Sync(); err != nil {
			return &syncErr{cause: err}
		}
	}
	w.stats.addSync()
	w.stats.storeLastSync(time.Now().UnixNano())
	w.durable.advance(currentLSN)
	return nil
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
