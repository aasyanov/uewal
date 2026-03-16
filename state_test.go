package uewal

import (
	"sync"
	"testing"
)

func TestState_String(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{StateInit, "INIT"},
		{StateRunning, "RUNNING"},
		{StateDraining, "DRAINING"},
		{StateClosed, "CLOSED"},
		{State(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("State(%d).String()=%q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestStateMachine_ValidTransitions(t *testing.T) {
	sm := &stateMachine{}
	if sm.load() != StateInit {
		t.Fatalf("initial state=%v, want INIT", sm.load())
	}
	if !sm.transition(StateInit, StateRunning) {
		t.Error("INIT→RUNNING: transition failed")
	}
	if !sm.transition(StateRunning, StateDraining) {
		t.Error("RUNNING→DRAINING: transition failed")
	}
	if !sm.transition(StateDraining, StateClosed) {
		t.Error("DRAINING→CLOSED: transition failed")
	}
	if sm.load() != StateClosed {
		t.Errorf("final state=%v, want CLOSED", sm.load())
	}
}

func TestStateMachine_InvalidTransitions(t *testing.T) {
	sm := &stateMachine{}

	// Backward: RUNNING→INIT
	sm.transition(StateInit, StateRunning)
	if sm.transition(StateRunning, StateInit) {
		t.Error("RUNNING→INIT: should fail")
	}

	// Skip: INIT→CLOSED
	sm2 := &stateMachine{}
	if sm2.transition(StateInit, StateClosed) {
		t.Error("INIT→CLOSED: should fail")
	}

	// CLOSED→RUNNING
	sm.transition(StateRunning, StateDraining)
	sm.transition(StateDraining, StateClosed)
	if sm.transition(StateClosed, StateRunning) {
		t.Error("CLOSED→RUNNING: should fail")
	}

	// INIT→DRAINING
	sm3 := &stateMachine{}
	if sm3.transition(StateInit, StateDraining) {
		t.Error("INIT→DRAINING: should fail")
	}

	// RUNNING→CLOSED
	sm4 := &stateMachine{}
	sm4.transition(StateInit, StateRunning)
	if sm4.transition(StateRunning, StateClosed) {
		t.Error("RUNNING→CLOSED: should fail")
	}
}

func TestStateMachine_ConcurrentTransition(t *testing.T) {
	sm := &stateMachine{}
	var successCount int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if sm.transition(StateInit, StateRunning) {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if successCount != 1 {
		t.Errorf("exactly 1 goroutine should succeed; got %d", successCount)
	}
	if sm.load() != StateRunning {
		t.Errorf("state=%v, want RUNNING", sm.load())
	}
}

func TestStateMachine_MustBeRunning(t *testing.T) {
	sm := &stateMachine{}
	if err := sm.mustBeRunning(); err != ErrNotRunning {
		t.Errorf("INIT: mustBeRunning()=%v, want ErrNotRunning", err)
	}

	sm.transition(StateInit, StateRunning)
	if err := sm.mustBeRunning(); err != nil {
		t.Errorf("RUNNING: mustBeRunning()=%v, want nil", err)
	}

	sm.transition(StateRunning, StateDraining)
	if err := sm.mustBeRunning(); err != ErrDraining {
		t.Errorf("DRAINING: mustBeRunning()=%v, want ErrDraining", err)
	}

	sm.transition(StateDraining, StateClosed)
	if err := sm.mustBeRunning(); err != ErrClosed {
		t.Errorf("CLOSED: mustBeRunning()=%v, want ErrClosed", err)
	}
}
