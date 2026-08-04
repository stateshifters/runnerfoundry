package cluster

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/meza/RunnerFoundry/internal/scaler"
)

func (store *Store) MarkRunnerBusy(
	ctx context.Context,
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
	githubRunnerID int64,
	startedAt time.Time,

) error {
	if err := identity.Validate(); err != nil {
		return fmt.Errorf("validate clustered runner busy identity: %w", err)
	}
	return store.runClusterRunnerIdentityTransaction(ctx, "clustered runner busy transition",
		func(transaction *databaseTransaction) error {
			return markClusterRunnerBusyInTransaction(
				ctx, transaction, scaleSet, repository, identity, githubRunnerID, startedAt,
			)
		})
}

func markClusterRunnerBusyInTransaction(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
	githubRunnerID int64,
	startedAt time.Time,
) error {
	predicate, err := newClusterRunnerIdentityPredicate(identity, 5)
	if err != nil {
		return err
	}
	statement := fmt.Sprintf(`UPDATE scaler_runner_lifecycle
		SET phase = 'busy', github_runner_id = CASE WHEN $1 > 0 THEN $1 ELSE github_runner_id END,
		    waiting_since = COALESCE(waiting_since, $2), updated_at = $2
		WHERE scale_set = $3 AND repository = $4 AND phase <> 'reap_pending'
		  AND %s`, predicate.condition)
	arguments := append([]any{githubRunnerID, startedAt, scaleSet, repository}, predicate.arguments...)
	commandTag, err := transaction.Exec(ctx, statement, arguments...)
	if err != nil {
		return fmt.Errorf("mark clustered runner busy: %w", err)
	}
	scope := clusterRunnerIdentityScope{
		table: "scaler_runner_lifecycle", condition: "scale_set = $1 AND repository = $2 AND phase <> 'reap_pending'",
		arguments: []any{scaleSet, repository},
	}
	return requireClusterRunnerIdentityMatch(ctx, transaction, scope, identity, commandTag.RowsAffected())
}

func (store *Store) BindRunnerIdentity(
	ctx context.Context,
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
	githubRunnerID int64,
	startedAt time.Time,
) error {
	if err := identity.Validate(); err != nil || identity.RequestID <= 0 || strings.TrimSpace(identity.RunnerName) == "" {
		return scaler.ErrInvalidRunnerIdentity
	}
	return store.runClusterRunnerIdentityTransaction(ctx, "clustered runner identity binding",
		func(transaction *databaseTransaction) error {
			return bindClusterRunnerIdentityInTransaction(
				ctx, transaction, scaleSet, repository, identity, githubRunnerID, startedAt,
			)
		})
}

func bindClusterRunnerIdentityInTransaction(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
	githubRunnerID int64,
	startedAt time.Time,
) error {
	if err := identity.Validate(); err != nil || identity.RequestID <= 0 || strings.TrimSpace(identity.RunnerName) == "" {
		return scaler.ErrInvalidRunnerIdentity
	}
	predicate := clusterRunnerIdentityPredicateFor(identity, 5)
	matches, err := markExactClusterRunnerBusy(
		ctx, transaction, scaleSet, repository, identity, githubRunnerID, startedAt, predicate,
	)
	if err != nil {
		return err
	}
	scope := clusterRunnerIdentityScope{
		table: "scaler_runner_lifecycle", condition: "scale_set = $1 AND repository = $2 AND phase <> 'reap_pending'",
		arguments: []any{scaleSet, repository},
	}
	if matches == 0 {
		if err = bindClusterRunnerIdentity(ctx, transaction, scope, scaleSet, repository,
			identity, githubRunnerID, startedAt); err != nil {
			return err
		}
	}
	return mirrorBoundClusterLibvirtIdentity(ctx, transaction, scaleSet, repository, identity)
}

func markExactClusterRunnerBusy(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
	githubRunnerID int64,
	startedAt time.Time,
	predicate clusterRunnerIdentityPredicate,
) (int64, error) {
	statement := fmt.Sprintf(`UPDATE scaler_runner_lifecycle
		SET phase = 'busy', github_runner_id = CASE WHEN $1 > 0 THEN $1 ELSE github_runner_id END,
		    waiting_since = COALESCE(waiting_since, $2), updated_at = $2
		WHERE scale_set = $3 AND repository = $4 AND phase <> 'reap_pending' AND %s`, predicate.condition)
	arguments := append([]any{githubRunnerID, startedAt, scaleSet, repository}, predicate.arguments...)
	commandTag, err := transaction.Exec(ctx, statement, arguments...)
	if err != nil {
		return 0, fmt.Errorf("mark exact clustered runner busy: %w", err)
	}
	matches := commandTag.RowsAffected()
	if matches > 1 {
		return 0, &scaler.RunnerIdentityInvariantError{Identity: identity, Matches: matches}
	}
	return matches, nil
}

func (store *Store) RequestRunnerReap(
	ctx context.Context,
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
	reason string,
	requestedAt time.Time,
) error {
	predicate, err := newClusterRunnerIdentityPredicate(identity, 5)
	if err != nil {
		return err
	}
	return store.runClusterRunnerIdentityTransaction(ctx, "clustered runner reap request",
		func(transaction *databaseTransaction) error {
			return requestClusterRunnerReapInTransaction(
				ctx, transaction, scaleSet, repository, identity, reason, requestedAt, predicate,
			)
		})
}

func requestClusterRunnerReapInTransaction(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
	reason string,
	requestedAt time.Time,
	predicate clusterRunnerIdentityPredicate,
) error {
	statement := fmt.Sprintf(`UPDATE scaler_runner_lifecycle
		SET phase = 'reap_pending', reap_reason = $1, updated_at = $2
		WHERE scale_set = $3 AND repository = $4 AND %s`, predicate.condition)
	arguments := append([]any{reason, requestedAt, scaleSet, repository}, predicate.arguments...)
	commandTag, err := transaction.Exec(ctx, statement, arguments...)
	if err != nil {
		return fmt.Errorf("request clustered runner reap: %w", err)
	}
	scope := clusterRunnerIdentityScope{
		table: "scaler_runner_lifecycle", condition: "scale_set = $1 AND repository = $2",
		arguments: []any{scaleSet, repository},
	}
	if err = requireClusterRunnerIdentityMatch(ctx, transaction, scope, identity, commandTag.RowsAffected()); err != nil {
		return err
	}
	return mirrorClusterRunnerReap(ctx, transaction, scaleSet, repository, identity)
}

func mirrorClusterRunnerReap(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
) error {
	predicate, err := newClusterRunnerIdentityPredicate(identity, 3)
	if err != nil {
		return err
	}
	statement := fmt.Sprintf(`UPDATE scaler_libvirt_runner_lifecycle SET reap_requested = true
		WHERE scale_set = $1 AND repository = $2 AND %s`, predicate.condition)
	arguments := append([]any{scaleSet, repository}, predicate.arguments...)
	commandTag, err := transaction.Exec(ctx, statement, arguments...)
	if err != nil {
		return fmt.Errorf("mirror clustered libvirt runner reap: %w", err)
	}
	return requireAtMostOneClusterRunnerIdentityMatch(identity, commandTag.RowsAffected())
}

func bindClusterRunnerIdentity(
	ctx context.Context,
	transaction *databaseTransaction,
	scope clusterRunnerIdentityScope,
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
	githubRunnerID int64,
	startedAt time.Time,
) error {
	matches, err := inspectClusterBindingCandidates(ctx, transaction, scope, identity)
	if err != nil {
		return err
	}
	if err = requireBindableClusterRunner(identity, matches); err != nil {
		return err
	}
	return updateBoundClusterRunner(ctx, transaction, scaleSet, repository, identity, githubRunnerID, startedAt)
}

type clusterBindingCandidates struct {
	request int64
	runner  int64
}

func inspectClusterBindingCandidates(
	ctx context.Context,
	transaction *databaseTransaction,
	scope clusterRunnerIdentityScope,
	identity scaler.RunnerIdentity,
) (clusterBindingCandidates, error) {
	requestMatches, err := countClusterRunnerIdentityComponent(ctx, transaction, scope, "request_id", identity.RequestID)
	if err != nil {
		return clusterBindingCandidates{}, err
	}
	runnerMatches, err := countClusterRunnerIdentityComponent(ctx, transaction, scope, "runner_name", identity.RunnerName)
	if err != nil {
		return clusterBindingCandidates{}, err
	}
	return clusterBindingCandidates{request: requestMatches, runner: runnerMatches}, nil
}

func requireBindableClusterRunner(identity scaler.RunnerIdentity, matches clusterBindingCandidates) error {
	if matches.request > 1 || matches.runner > 1 {
		return &scaler.RunnerIdentityInvariantError{Identity: identity, Matches: max(matches.request, matches.runner)}
	}
	if matches.request > 0 {
		return &scaler.RunnerIdentityMismatchError{Identity: identity}
	}
	if matches.runner == 0 {
		return &scaler.RunnerIdentityNotFoundError{Identity: identity}
	}
	return nil
}

func updateBoundClusterRunner(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
	githubRunnerID int64,
	startedAt time.Time,
) error {
	commandTag, err := transaction.Exec(ctx, `UPDATE scaler_runner_lifecycle
		SET request_id = $1, phase = 'busy',
		    github_runner_id = CASE WHEN $2 > 0 THEN $2 ELSE github_runner_id END,
		    waiting_since = COALESCE(waiting_since, $3), updated_at = $3
		WHERE scale_set = $4 AND repository = $5 AND runner_name = $6 AND request_id = 0
		  AND phase <> 'reap_pending'`, identity.RequestID, githubRunnerID, startedAt,
		scaleSet, repository, identity.RunnerName)
	if err != nil {
		return fmt.Errorf("bind clustered runner identity: %w", err)
	}
	if commandTag.RowsAffected() > 1 {
		return &scaler.RunnerIdentityInvariantError{Identity: identity, Matches: commandTag.RowsAffected()}
	}
	if commandTag.RowsAffected() == 0 {
		return &scaler.RunnerIdentityMismatchError{Identity: identity}
	}
	return nil
}

func mirrorBoundClusterLibvirtIdentity(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
) error {
	commandTag, err := transaction.Exec(ctx, `UPDATE scaler_libvirt_runner_lifecycle candidate
		SET request_id = $1
		WHERE candidate.scale_set = $2 AND candidate.repository = $3
		  AND candidate.runner_name = $4 AND candidate.request_id = 0
		  AND NOT EXISTS (
			SELECT 1 FROM scaler_libvirt_runner_lifecycle owner
			WHERE owner.scale_set = $2 AND owner.repository = $3 AND owner.request_id = $1
		  )`, identity.RequestID, scaleSet, repository, identity.RunnerName)
	if err != nil {
		return fmt.Errorf("bind clustered libvirt runner request identity: %w", err)
	}
	if commandTag.RowsAffected() > 1 {
		return &scaler.RunnerIdentityInvariantError{Identity: identity, Matches: commandTag.RowsAffected()}
	}
	if commandTag.RowsAffected() == 1 {
		return nil
	}
	return classifyUnchangedClusterLibvirtIdentity(ctx, transaction, scaleSet, repository, identity)
}

func classifyUnchangedClusterLibvirtIdentity(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
) error {
	scope := clusterRunnerIdentityScope{
		table: "scaler_libvirt_runner_lifecycle", condition: "scale_set = $1 AND repository = $2",
		arguments: []any{scaleSet, repository},
	}
	matches, err := inspectClusterBindingCandidates(ctx, transaction, scope, identity)
	if err != nil {
		return err
	}
	if matches.request > 1 || matches.runner > 1 {
		return &scaler.RunnerIdentityInvariantError{Identity: identity, Matches: max(matches.request, matches.runner)}
	}
	if matches.request == 0 && matches.runner == 0 {
		return nil
	}
	exactMatches, err := countExactClusterLibvirtIdentity(ctx, transaction, scaleSet, repository, identity)
	if err != nil {
		return err
	}
	if exactMatches == 1 && matches.request == 1 && matches.runner == 1 {
		return nil
	}
	return &scaler.RunnerIdentityMismatchError{Identity: identity}
}

func countExactClusterLibvirtIdentity(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
) (int64, error) {
	predicate, err := newClusterRunnerIdentityPredicate(identity, 3)
	if err != nil {
		return 0, err
	}
	var exactMatches int64
	exactArguments := append([]any{scaleSet, repository}, predicate.arguments...)
	if err = transaction.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM scaler_libvirt_runner_lifecycle
		WHERE scale_set = $1 AND repository = $2 AND %s`, predicate.condition), exactArguments...).Scan(&exactMatches); err != nil {
		return 0, fmt.Errorf("inspect clustered libvirt runner identity: %w", err)
	}
	if exactMatches > 1 {
		return 0, &scaler.RunnerIdentityInvariantError{Identity: identity, Matches: exactMatches}
	}
	return exactMatches, nil
}
