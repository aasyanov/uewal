package uewal

import "sync/atomic"

// State represents the lifecycle state of the WAL.
//
// The state machine enforces a strict forward-only progression:
//
//	StateInit → StateRunning → StateDraining → StateClosed
//
// Backward transitions are never allowed. Transitions are atomic
// (compare-and-swap) and safe for concurrent observation.
type State int32

const (
	// StateInit is the initial state. The WAL has been allocated but the
	// writer goroutine has not started. Transitions to StateRunning on Open.
	StateInit State = iota

	// StateRunning indicates the WAL is fully operational: accepting appends,
	// serving replays, and collecting statistics.
	StateRunning

	// StateDraining indicates a graceful shutdown has been initiated.
	// New appends are rejected (ErrDraining), but the writer goroutine
	// continues to drain queued batches.
	StateDraining

	// StateClosed is the terminal state. All resources have been released.
	// Read-only accessors ([WAL.Stats], [WAL.State], [WAL.Dir], [WAL.FirstLSN],
	// [WAL.LastLSN]) remain usable; write and replay operations return errors.
	StateClosed
)

// String returns a human-readable name for the state.
func (s State) String() string {
	switch s {
	case StateInit:
		return "INIT"
	case StateRunning:
		return "RUNNING"
	case StateDraining:
		return "DRAINING"
	case StateClosed:
		return "CLOSED"
	default:
		return "UNKNOWN"
	}
}

// stateMachine manages WAL lifecycle transitions atomically.
// It enforces that only valid forward transitions occur.
type stateMachine struct {
	state atomic.Int32
}

// load returns the current state.
func (sm *stateMachine) load() State {
	return State(sm.state.Load())
}

// transition performs a validated CAS from expected to next.
// Only valid forward transitions are allowed:
//
//	INIT → RUNNING → DRAINING → CLOSED
//
// Returns true on success, false if the current state does not match
// from or the transition is not in the valid set.
func (sm *stateMachine) transition(from, to State) bool {
	if !validTransition(from, to) {
		return false
	}
	return sm.state.CompareAndSwap(int32(from), int32(to))
}

// validTransition returns true if from→to is a permitted lifecycle transition.
func validTransition(from, to State) bool {
	switch from {
	case StateInit:
		return to == StateRunning
	case StateRunning:
		return to == StateDraining
	case StateDraining:
		return to == StateClosed
	default:
		return false
	}
}

// mustBeRunning returns nil if the WAL is in StateRunning, or an appropriate
// sentinel error for other states.
func (sm *stateMachine) mustBeRunning() error {
	switch sm.load() {
	case StateRunning:
		return nil
	case StateDraining:
		return ErrDraining
	case StateClosed:
		return ErrClosed
	default:
		return ErrNotRunning
	}
}
