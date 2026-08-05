package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type leaderLease interface {
	Alive(ctx context.Context) bool
	Close(ctx context.Context) error
}

type tryLeader func(context.Context) (leaderLease, error)

type exclusiveLeadership struct {
	notLeader error
	try       tryLeader
	lease     leaderLease
}

func newExclusiveLeadership(notLeader error, try tryLeader) *exclusiveLeadership {
	return &exclusiveLeadership{notLeader: notLeader, try: try}
}

func (leadership *exclusiveLeadership) IsLeader(ctx context.Context) (bool, error) {
	if leadership.lease != nil && leadership.lease.Alive(ctx) {
		return true, nil
	}
	if leadership.lease != nil {
		if err := leadership.lease.Close(ctx); err != nil {
			return false, fmt.Errorf("close lost leader lease: %w", err)
		}
		leadership.lease = nil
	}
	lease, err := leadership.try(ctx)
	if errors.Is(err, leadership.notLeader) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("acquire leader lease: %w", err)
	}
	leadership.lease = lease
	return true, nil
}

func (leadership *exclusiveLeadership) Close(ctx context.Context) error {
	if leadership.lease == nil {
		return nil
	}
	lease := leadership.lease
	leadership.lease = nil
	if err := lease.Close(ctx); err != nil {
		return fmt.Errorf("close leader lease: %w", err)
	}
	return nil
}

type permanentLeadership struct{}

func (permanentLeadership) IsLeader(context.Context) (bool, error) { return true, nil }

// localLeadershipGate prevents overlapping runtime generations in one
// process. A replacement remains inactive until the previous generation has
// closed, including SQLite where no database advisory lock exists.
type localLeadershipGate struct {
	mutex sync.Mutex
	owner *localLeadership
}

func (gate *localLeadershipGate) NewLeadership() *localLeadership {
	return &localLeadership{gate: gate}
}

type localLeadership struct {
	gate *localLeadershipGate
}

func (leadership *localLeadership) IsLeader(context.Context) (bool, error) {
	leadership.gate.mutex.Lock()
	defer leadership.gate.mutex.Unlock()
	if leadership.gate.owner == nil {
		leadership.gate.owner = leadership
	}
	return leadership.gate.owner == leadership, nil
}

func (leadership *localLeadership) Close(context.Context) error {
	leadership.gate.mutex.Lock()
	defer leadership.gate.mutex.Unlock()
	if leadership.gate.owner == leadership {
		leadership.gate.owner = nil
	}
	return nil
}
