package uewal

import (
	"testing"
)

func TestStateString(t *testing.T) {
	cases := []struct {
		s    State
		want string
	}{
		{StateInit, "INIT"},
		{StateRunning, "RUNNING"},
		{StateDraining, "DRAINING"},
		{StateClosed, "CLOSED"},
		{State(99), "UNKNOWN"},
	}
	for _, tc := range cases {
		if got := tc.s.String(); got != tc.want {
			t.Errorf("State(%d).String()=%q, want %q", tc.s, got, tc.want)
		}
	}
}

func TestStateMachineValidTransitions(t *testing.T) {
	var sm stateMachine
	if sm.load() != StateInit {
		t.Fatalf("initial state=%v, want INIT", sm.load())
	}

	if !sm.transition(StateInit, StateRunning) {
		t.Fatal("INIT->RUNNING should succeed")
	}
	if sm.load() != StateRunning {
		t.Fatalf("state=%v, want RUNNING", sm.load())
	}

	if !sm.transition(StateRunning, StateDraining) {
		t.Fatal("RUNNING->DRAINING should succeed")
	}
	if sm.load() != StateDraining {
		t.Fatalf("state=%v, want DRAINING", sm.load())
	}

	if !sm.transition(StateDraining, StateClosed) {
		t.Fatal("DRAINING->CLOSED should succeed")
	}
	if sm.load() != StateClosed {
		t.Fatalf("state=%v, want CLOSED", sm.load())
	}
}

func TestStateMachineInvalidTransitions(t *testing.T) {
	cases := []struct {
		name     string
		from, to State
	}{
		{"INIT->DRAINING", StateInit, StateDraining},
		{"INIT->CLOSED", StateInit, StateClosed},
		{"RUNNING->INIT", StateRunning, StateInit},
		{"RUNNING->CLOSED", StateRunning, StateClosed},
		{"DRAINING->INIT", StateDraining, StateInit},
		{"DRAINING->RUNNING", StateDraining, StateRunning},
		{"CLOSED->INIT", StateClosed, StateInit},
		{"CLOSED->RUNNING", StateClosed, StateRunning},
		{"CLOSED->DRAINING", StateClosed, StateDraining},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var sm stateMachine
			// Advance to the "from" state via valid path
			switch tc.from {
			case StateRunning:
				sm.transition(StateInit, StateRunning)
			case StateDraining:
				sm.transition(StateInit, StateRunning)
				sm.transition(StateRunning, StateDraining)
			case StateClosed:
				sm.transition(StateInit, StateRunning)
				sm.transition(StateRunning, StateDraining)
				sm.transition(StateDraining, StateClosed)
			}

			if sm.transition(tc.from, tc.to) {
				t.Errorf("%s should have failed but succeeded", tc.name)
			}
		})
	}
}

func TestMustBeRunning(t *testing.T) {
	var sm stateMachine
	if err := sm.mustBeRunning(); err != ErrNotRunning {
		t.Errorf("INIT: got %v, want ErrNotRunning", err)
	}

	sm.transition(StateInit, StateRunning)
	if err := sm.mustBeRunning(); err != nil {
		t.Errorf("RUNNING: got %v, want nil", err)
	}

	sm.transition(StateRunning, StateDraining)
	if err := sm.mustBeRunning(); err != ErrDraining {
		t.Errorf("DRAINING: got %v, want ErrDraining", err)
	}

	sm.transition(StateDraining, StateClosed)
	if err := sm.mustBeRunning(); err != ErrClosed {
		t.Errorf("CLOSED: got %v, want ErrClosed", err)
	}
}

func TestStateMachineConcurrentTransition(t *testing.T) {
	var sm stateMachine
	sm.transition(StateInit, StateRunning)

	const goroutines = 100
	wins := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			wins <- sm.transition(StateRunning, StateDraining)
		}()
	}

	winCount := 0
	for i := 0; i < goroutines; i++ {
		if <-wins {
			winCount++
		}
	}

	if winCount != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", winCount)
	}
	if sm.load() != StateDraining {
		t.Fatalf("state=%v, want DRAINING", sm.load())
	}
}
