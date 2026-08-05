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
	arguments := append([]any{githubRunnerID, startedAt, scaleSet, repository}, predicate.arguments...)
	lockParameter := len(arguments) + 1
	mutation := fmt.Sprintf(`UPDATE scaler_runner_lifecycle
		SET phase = 'busy', github_runner_id = CASE WHEN $1 > 0 THEN $1 ELSE github_runner_id END,
		    waiting_since = COALESCE(waiting_since, $2), updated_at = $2
		FROM locked
		WHERE scale_set = $3 AND repository = $4 AND phase <> 'reap_pending'
		  AND %s`, predicate.condition)
	statement := clusterLifecycleLockedStatement(lockParameter, mutation)
	arguments = append(arguments, clusterLifecycleAdvisoryKey(scaleSet, repository, identity))
	commandTag, err := transaction.Exec(ctx, statement, arguments...)
	if err != nil {
		return fmt.Errorf("mark clustered runner busy: %w", err)
	}
	scope := clusterRunnerIdentityScope{
		table: "scaler_runner_lifecycle", condition: "scale_set = $1 AND repository = $2 AND phase <> 'reap_pending'",
		arguments: []any{scaleSet, repository},
	}
	if err = requireClusterRunnerIdentityMatch(ctx, transaction, scope, identity, commandTag.RowsAffected()); err != nil {
		return err
	}
	return reconcileClusterLibvirtLifecycle(ctx, transaction, scaleSet, repository, identity)
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
	arguments := append([]any{githubRunnerID, startedAt, scaleSet, repository}, predicate.arguments...)
	lockParameter := len(arguments) + 1
	statement := fmt.Sprintf(`WITH locked AS MATERIALIZED (
		SELECT pg_advisory_xact_lock(hashtextextended($%d, 0))
	)
	UPDATE scaler_runner_lifecycle
		SET phase = 'busy', github_runner_id = CASE WHEN $1 > 0 THEN $1 ELSE github_runner_id END,
		    waiting_since = COALESCE(waiting_since, $2), updated_at = $2
		FROM locked
		WHERE scale_set = $3 AND repository = $4 AND phase <> 'reap_pending' AND %s`,
		lockParameter, predicate.condition)
	arguments = append(arguments, clusterLifecycleAdvisoryKey(scaleSet, repository, identity))
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
	arguments := append([]any{reason, requestedAt, scaleSet, repository}, predicate.arguments...)
	lockParameter := len(arguments) + 1
	statement := fmt.Sprintf(`WITH locked AS MATERIALIZED (
		SELECT pg_advisory_xact_lock(hashtextextended($%d, 0))
	)
	UPDATE scaler_runner_lifecycle
		SET phase = 'reap_pending', reap_reason = $1, updated_at = $2
		FROM locked
		WHERE scale_set = $3 AND repository = $4 AND %s`, lockParameter, predicate.condition)
	arguments = append(arguments, clusterLifecycleAdvisoryKey(scaleSet, repository, identity))
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
	return reconcileClusterLibvirtLifecycle(ctx, transaction, scaleSet, repository, identity)
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
	commandTag, err := transaction.Exec(ctx, `WITH locked AS MATERIALIZED (
		SELECT pg_advisory_xact_lock(hashtextextended($7, 0))
	)
	UPDATE scaler_runner_lifecycle
		SET request_id = $1, phase = 'busy',
		    github_runner_id = CASE WHEN $2 > 0 THEN $2 ELSE github_runner_id END,
		    waiting_since = COALESCE(waiting_since, $3), updated_at = $3
		FROM locked
		WHERE scale_set = $4 AND repository = $5 AND runner_name = $6 AND request_id = 0
		  AND phase <> 'reap_pending'`, identity.RequestID, githubRunnerID, startedAt,
		scaleSet, repository, identity.RunnerName,
		clusterLifecycleAdvisoryKey(scaleSet, repository, identity))
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
	return reconcileClusterLibvirtLifecycle(ctx, transaction, scaleSet, repository, identity)
}

func reconcileClusterLibvirtLifecycle(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
) error {
	predicate, err := newClusterRunnerIdentityPredicate(identity, 3)
	if err != nil {
		return err
	}
	statement := fmt.Sprintf(reconcileClusterLibvirtLifecycleSQL, predicate.condition)
	arguments := append([]any{scaleSet, repository}, predicate.arguments...)
	commandTag, err := transaction.Exec(ctx, statement, arguments...)
	if err != nil {
		return fmt.Errorf("reconcile clustered libvirt lifecycle transition: %w", err)
	}
	if commandTag.RowsAffected() > 1 {
		return &scaler.RunnerIdentityInvariantError{Identity: identity, Matches: commandTag.RowsAffected()}
	}
	if commandTag.RowsAffected() == 1 {
		return nil
	}
	return classifyUnreconciledClusterLibvirtLifecycle(ctx, transaction, scaleSet, repository, identity)
}

func classifyUnreconciledClusterLibvirtLifecycle(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
) error {
	predicate, err := newClusterRunnerIdentityPredicate(identity, 3)
	if err != nil {
		return err
	}
	authority, err := loadClusterLifecycleTransitionAuthority(
		ctx, transaction, scaleSet, repository, predicate,
	)
	if err != nil {
		return err
	}
	mirrorCount, err := countClusterLifecycleTransitionMirrors(
		ctx, transaction, scaleSet, repository, authority,
	)
	if err != nil {
		return err
	}
	return classifyClusterLifecycleTransitionConflict(scaleSet, repository, identity, authority, mirrorCount)
}

type clusterLifecycleTransitionAuthority struct {
	nodeID     NodeID
	provider   scaler.RunnerProvider
	runnerName string
}

func loadClusterLifecycleTransitionAuthority(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository string,
	predicate clusterRunnerIdentityPredicate,
) (clusterLifecycleTransitionAuthority, error) {
	arguments := append([]any{scaleSet, repository}, predicate.arguments...)
	var authority clusterLifecycleTransitionAuthority
	err := transaction.QueryRow(ctx, fmt.Sprintf(`SELECT node_id, provider, runner_name FROM scaler_runner_lifecycle
		WHERE scale_set = $1 AND repository = $2 AND %s`, predicate.condition), arguments...).Scan(
		&authority.nodeID, &authority.provider, &authority.runnerName,
	)
	if err != nil {
		return authority, fmt.Errorf("inspect authoritative clustered lifecycle transition: %w", err)
	}
	return authority, nil
}

func countClusterLifecycleTransitionMirrors(
	ctx context.Context,
	transaction *databaseTransaction,
	scaleSet, repository string,
	authority clusterLifecycleTransitionAuthority,
) (int64, error) {
	var mirrorCount int64
	err := transaction.QueryRow(ctx, `SELECT COUNT(*) FROM scaler_libvirt_runner_lifecycle
		WHERE node_id = $1 AND scale_set = $2 AND repository = $3 AND runner_name = $4`,
		authority.nodeID, scaleSet, repository, authority.runnerName).Scan(&mirrorCount)
	if err != nil {
		return 0, fmt.Errorf("inspect legacy clustered lifecycle transition: %w", err)
	}
	return mirrorCount, nil
}

func classifyClusterLifecycleTransitionConflict(
	scaleSet, repository string,
	identity scaler.RunnerIdentity,
	authority clusterLifecycleTransitionAuthority,
	mirrorCount int64,
) error {
	if mirrorCount > 1 {
		return &scaler.RunnerIdentityInvariantError{Identity: identity, Matches: mirrorCount}
	}
	if authority.provider != scaler.RunnerProviderLibvirt && mirrorCount == 0 {
		return nil
	}
	reason := "legacy lifecycle conflicts with authoritative transition"
	if authority.provider != scaler.RunnerProviderLibvirt {
		reason = "legacy libvirt lifecycle conflicts with authoritative provider"
	}
	return &scaler.RunnerLifecycleConsistencyError{
		ScaleSet: scaleSet, Repository: repository, RunnerName: authority.runnerName,
		Reason: reason,
	}
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
