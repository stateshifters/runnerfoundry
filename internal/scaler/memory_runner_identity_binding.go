package scaler

import "time"

func (store *MemoryDemandStore) bindMemoryRunnerIdentity(
	scaleSet, repository string, identity RunnerIdentity, githubRunnerID int64, startedAt time.Time,
) error {
	selection := store.findMemoryIdentityFreeRunner(scaleSet, repository, identity)
	candidateKey, candidate, err := selection.result(identity)
	if err != nil {
		return err
	}
	store.markMemoryRunnerBusy(candidateKey, candidate, identity, githubRunnerID, startedAt)
	return nil
}

type memoryIdentityBindingSelection struct {
	candidateKey   string
	candidate      RunnerLifecycle
	nameMatches    int
	requestMatches int
}

func (store *MemoryDemandStore) findMemoryIdentityFreeRunner(
	scaleSet, repository string,
	identity RunnerIdentity,
) memoryIdentityBindingSelection {
	selection := memoryIdentityBindingSelection{}
	for key, runner := range store.runners {
		if runner.ScaleSet != scaleSet || runner.Repository != repository || !activeMemoryRunner(runner) {
			continue
		}
		selection.observe(key, runner, identity)
	}
	return selection
}

func (selection *memoryIdentityBindingSelection) observe(
	key string,
	runner RunnerLifecycle,
	identity RunnerIdentity,
) {
	if runner.RequestID == identity.RequestID {
		selection.requestMatches++
	}
	if runner.RunnerName != identity.RunnerName {
		return
	}
	selection.nameMatches++
	if runner.RequestID == 0 {
		selection.candidateKey = key
		selection.candidate = runner
	}
}

func (selection memoryIdentityBindingSelection) result(
	identity RunnerIdentity,
) (string, RunnerLifecycle, error) {
	if selection.nameMatches > 1 || selection.requestMatches > 1 {
		matches := max(selection.nameMatches, selection.requestMatches)
		return "", RunnerLifecycle{}, &RunnerIdentityInvariantError{Identity: identity, Matches: int64(matches)}
	}
	if selection.requestMatches > 0 || selection.nameMatches > 0 && selection.candidateKey == "" {
		return "", RunnerLifecycle{}, &RunnerIdentityMismatchError{Identity: identity}
	}
	if selection.candidateKey == "" {
		return "", RunnerLifecycle{}, &RunnerIdentityNotFoundError{Identity: identity}
	}
	return selection.candidateKey, selection.candidate, nil
}
