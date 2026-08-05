package scaler

import (
	"context"
	"errors"
	"time"
)

func (store *MemoryDemandStore) MarkRunnerBusy(
	_ context.Context, scaleSet, repository string, identity RunnerIdentity, githubRunnerID int64, startedAt time.Time,
) error {
	key, runner, err := store.resolveMemoryRunner(scaleSet, repository, identity, activeMemoryRunner)
	if err != nil {
		return err
	}
	store.markMemoryRunnerBusy(key, runner, identity, githubRunnerID, startedAt)
	return nil
}

func (store *MemoryDemandStore) BindRunnerIdentity(
	_ context.Context, scaleSet, repository string, identity RunnerIdentity, githubRunnerID int64, startedAt time.Time,
) error {
	if err := identity.Validate(); err != nil || !identity.isPair() {
		return ErrInvalidRunnerIdentity
	}
	key, runner, err := store.resolveMemoryRunner(scaleSet, repository, identity, activeMemoryRunner)
	if err == nil {
		store.markMemoryRunnerBusy(key, runner, identity, githubRunnerID, startedAt)
		return nil
	}
	if !errors.Is(err, ErrRunnerIdentityMismatch) && !errors.Is(err, ErrRunnerIdentityNotFound) {
		return err
	}
	return store.bindMemoryRunnerIdentity(scaleSet, repository, identity, githubRunnerID, startedAt)
}

type memoryRunnerEligibility func(RunnerLifecycle) bool

func activeMemoryRunner(runner RunnerLifecycle) bool { return runner.Phase != RunnerReapPending }

func anyMemoryRunner(RunnerLifecycle) bool { return true }

func (store *MemoryDemandStore) resolveMemoryRunner(
	scaleSet, repository string,
	identity RunnerIdentity,
	eligible memoryRunnerEligibility,
) (string, RunnerLifecycle, error) {
	if err := identity.Validate(); err != nil {
		return "", RunnerLifecycle{}, err
	}
	selection := memoryIdentitySelection{}
	selectedKey := ""
	selectedRunner := RunnerLifecycle{}
	for key, runner := range store.runners {
		if runner.ScaleSet != scaleSet || runner.Repository != repository || !eligible(runner) {
			continue
		}
		if selection.observe(identity, runner.RequestID, runner.RunnerName) {
			selectedKey = key
			selectedRunner = runner
		}
	}
	if err := selection.result(identity); err != nil {
		return "", RunnerLifecycle{}, err
	}
	return selectedKey, selectedRunner, nil
}

type memoryIdentitySelection struct {
	exactMatches   int
	requestMatches int
	nameMatches    int
}

func (selection *memoryIdentitySelection) observe(identity RunnerIdentity, requestID int64, runnerName string) bool {
	requestMatches := identity.hasRequestID() && requestID == identity.RequestID
	nameMatches := identity.hasRunnerName() && runnerName == identity.RunnerName
	if requestMatches {
		selection.requestMatches++
	}
	if nameMatches {
		selection.nameMatches++
	}
	exactMatch := requestMatches || nameMatches
	if identity.isPair() {
		exactMatch = requestMatches && nameMatches
	}
	if exactMatch {
		selection.exactMatches++
	}
	return exactMatch
}

func (selection memoryIdentitySelection) result(identity RunnerIdentity) error {
	if selection.exactMatches > 1 {
		return &RunnerIdentityInvariantError{Identity: identity, Matches: int64(selection.exactMatches)}
	}
	if selection.exactMatches == 1 {
		return nil
	}
	if identity.isPair() && (selection.requestMatches > 0 || selection.nameMatches > 0) {
		return &RunnerIdentityMismatchError{Identity: identity}
	}
	return &RunnerIdentityNotFoundError{Identity: identity}
}

func (store *MemoryDemandStore) markMemoryRunnerBusy(
	key string, runner RunnerLifecycle, identity RunnerIdentity, githubRunnerID int64, startedAt time.Time,
) {
	runner.Phase = RunnerBusy
	if runner.RequestID == 0 && identity.RequestID > 0 {
		runner.RequestID = identity.RequestID
	}
	if githubRunnerID > 0 {
		runner.GitHubRunnerID = githubRunnerID
	}
	runner.UpdatedAt = startedAt
	store.runners[key] = runner
}
