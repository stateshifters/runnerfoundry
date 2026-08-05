package scaler

import (
	"context"
	"errors"
	"testing"
)

func TestSQLiteRegistrationRejectsRequestOwnedByGenericDockerRunner(t *testing.T) {
	store := newInMemorySQLiteLifecycleStore(t)
	owner := consistentSQLiteLibvirtRunner()
	owner.Provider = RunnerProviderDocker
	owner.RunnerName = "docker-owner"
	seedSQLiteLifecycleState(t, store.database, "generic-only", owner)
	candidate := owner
	candidate.RunnerName = "docker-candidate"

	err := store.RegisterRunner(context.Background(), candidate)
	if !errors.Is(err, ErrRunnerLifecycleConsistency) {
		t.Fatalf("RegisterRunner() error = %v, want lifecycle consistency error", err)
	}
	var ownerCount, candidateCount int
	if scanErr := store.database.QueryRow(`SELECT COUNT(*) FROM runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		owner.ScaleSet, owner.Repository, owner.RunnerName).Scan(&ownerCount); scanErr != nil {
		t.Fatal(scanErr)
	}
	if scanErr := store.database.QueryRow(`SELECT COUNT(*) FROM runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ?`,
		candidate.ScaleSet, candidate.Repository, candidate.RunnerName).Scan(&candidateCount); scanErr != nil {
		t.Fatal(scanErr)
	}
	if ownerCount != 1 || candidateCount != 0 {
		t.Fatalf("generic ownership mutated: owner=%d candidate=%d", ownerCount, candidateCount)
	}
}
