package scaler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type sqliteRunnerEligibility int

type sqliteIdentityQueries struct {
	request string
	name    string
	pair    string
}

type sqliteBindingCounts struct {
	request      int64
	name         int64
	identityFree int64
}

type sqliteLibvirtBindingCounts struct {
	name         int64
	exact        int64
	request      int64
	identityFree int64
}

const (
	activeSQLiteRunner sqliteRunnerEligibility = iota
	anySQLiteRunner
	pendingSQLiteRunner

	sqliteRequestIdentityClause = "request_id = ?"
	sqliteNameIdentityClause    = "runner_name = ?"
	sqlitePairIdentityClause    = "request_id = ? AND runner_name = ?"

	sqliteRunnerCountPrefix  = "SELECT COUNT(*) FROM runner_lifecycle WHERE scale_set = ? AND repository = ? AND "
	sqliteLibvirtCountPrefix = "SELECT COUNT(*) FROM libvirt_runner_lifecycle WHERE scale_set = ? AND repository = ? AND "
	sqliteActiveClause       = "phase <> 'reap_pending' AND "
	sqlitePendingClause      = "phase = 'reap_pending' AND "
	sqliteLibvirtPending     = "reap_requested = 1 AND "
)

func sqliteIdentityStatement(
	identity RunnerIdentity,
	requestStatement, nameStatement, pairStatement string,
) (string, []any, error) {
	if err := identity.Validate(); err != nil {
		return "", nil, err
	}
	statement, arguments := sqliteValidatedIdentityStatement(identity, requestStatement, nameStatement, pairStatement)
	return statement, arguments, nil
}

func sqliteValidatedIdentityStatement(
	identity RunnerIdentity,
	requestStatement, nameStatement, pairStatement string,
) (string, []any) {
	if identity.isPair() {
		return pairStatement, []any{identity.RequestID, identity.RunnerName}
	}
	if identity.hasRequestID() {
		return requestStatement, []any{identity.RequestID}
	}
	return nameStatement, []any{identity.RunnerName}
}

func resolveSQLiteRunnerIdentity(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	eligibility sqliteRunnerEligibility,
	identity RunnerIdentity,
) error {
	return resolveSQLiteIdentity(identity, func(candidate RunnerIdentity) (int64, error) {
		return countSQLiteRunnerMatches(ctx, transaction, eligibility, scaleSet, repository, candidate)
	})
}

func countSQLiteRunnerMatches(
	ctx context.Context,
	transaction *sql.Tx,
	eligibility sqliteRunnerEligibility,
	scaleSet, repository string,
	identity RunnerIdentity,
) (int64, error) {
	queries := sqliteRunnerCountQueries(eligibility)
	return countSQLiteIdentityMatches(ctx, transaction, identity, scaleSet, repository,
		queries, "resolve runner identity")
}

func resolveSQLiteLibvirtIdentity(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	eligibility sqliteRunnerEligibility,
	identity RunnerIdentity,
) error {
	return resolveSQLiteIdentity(identity, func(candidate RunnerIdentity) (int64, error) {
		return countSQLiteLibvirtMatches(ctx, transaction, eligibility, scaleSet, repository, candidate)
	})
}

func countSQLiteLibvirtMatches(
	ctx context.Context,
	transaction *sql.Tx,
	eligibility sqliteRunnerEligibility,
	scaleSet, repository string,
	identity RunnerIdentity,
) (int64, error) {
	queries := sqliteLibvirtCountQueries(eligibility)
	return countSQLiteIdentityMatches(ctx, transaction, identity, scaleSet, repository,
		queries, "resolve libvirt runner identity")
}

func resolveSQLiteIdentity(identity RunnerIdentity, count func(RunnerIdentity) (int64, error)) error {
	matches, err := count(identity)
	if err != nil {
		return err
	}
	if matches > 1 {
		return &RunnerIdentityInvariantError{Identity: identity, Matches: matches}
	}
	if matches == 1 {
		return nil
	}
	return classifyMissingSQLiteIdentity(identity, count)
}

func classifyMissingSQLiteIdentity(identity RunnerIdentity, count func(RunnerIdentity) (int64, error)) error {
	if !identity.isPair() {
		return &RunnerIdentityNotFoundError{Identity: identity}
	}
	requestMatches, err := count(RunnerIdentity{RequestID: identity.RequestID})
	if err != nil {
		return err
	}
	nameMatches, err := count(RunnerIdentity{RunnerName: identity.RunnerName})
	if err != nil {
		return err
	}
	if requestMatches > 0 || nameMatches > 0 {
		return &RunnerIdentityMismatchError{Identity: identity}
	}
	return &RunnerIdentityNotFoundError{Identity: identity}
}

func countSQLiteIdentityMatches(
	ctx context.Context,
	transaction *sql.Tx,
	identity RunnerIdentity,
	scaleSet, repository string,
	queries sqliteIdentityQueries,
	operation string,
) (int64, error) {
	query, identityArguments, err := sqliteIdentityStatement(identity, queries.request, queries.name, queries.pair)
	if err != nil {
		return 0, err
	}
	var matches int64
	arguments := append([]any{scaleSet, repository}, identityArguments...)
	if err = transaction.QueryRowContext(ctx, query, arguments...).Scan(&matches); err != nil {
		return 0, fmt.Errorf("%s: %w", operation, err)
	}
	return matches, nil
}

func sqliteRunnerCountQueries(eligibility sqliteRunnerEligibility) sqliteIdentityQueries {
	prefix := sqliteRunnerCountPrefix
	switch eligibility {
	case activeSQLiteRunner:
		prefix += sqliteActiveClause
	case anySQLiteRunner:
	case pendingSQLiteRunner:
		prefix += sqlitePendingClause
	}
	return sqliteIdentityQueries{
		request: prefix + sqliteRequestIdentityClause,
		name:    prefix + sqliteNameIdentityClause,
		pair:    prefix + sqlitePairIdentityClause,
	}
}

func sqliteLibvirtCountQueries(eligibility sqliteRunnerEligibility) sqliteIdentityQueries {
	prefix := sqliteLibvirtCountPrefix
	if eligibility == pendingSQLiteRunner {
		prefix += sqliteLibvirtPending
	}
	return sqliteIdentityQueries{
		request: prefix + sqliteRequestIdentityClause,
		name:    prefix + sqliteNameIdentityClause,
		pair:    prefix + sqlitePairIdentityClause,
	}
}

func requireSingleSQLiteIdentityChange(result sql.Result, identity RunnerIdentity, operation string) error {
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	if changed == 0 {
		return &RunnerIdentityNotFoundError{Identity: identity}
	}
	if changed > 1 {
		return &RunnerIdentityInvariantError{Identity: identity, Matches: changed}
	}
	return nil
}

func requireAtMostOneSQLiteIdentityChange(result sql.Result, identity RunnerIdentity, operation string) error {
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	if changed > 1 {
		return &RunnerIdentityInvariantError{Identity: identity, Matches: changed}
	}
	return nil
}

func executeSQLiteIdentityStatement(
	ctx context.Context,
	transaction *sql.Tx,
	identity RunnerIdentity,
	arguments []any,
	requestStatement, nameStatement, pairStatement, operation string,
) (sql.Result, error) {
	query, identityArguments, err := sqliteIdentityStatement(
		identity, requestStatement, nameStatement, pairStatement,
	)
	if err != nil {
		return nil, err
	}
	result, err := transaction.ExecContext(ctx, query, append(arguments, identityArguments...)...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	return result, nil
}

func (store *SQLiteDemandStore) BindRunnerIdentity(
	ctx context.Context,
	scaleSet, repository string,
	identity RunnerIdentity,
	githubRunnerID int64,
	startedAt time.Time,
) error {
	if err := identity.Validate(); err != nil || !identity.isPair() {
		return ErrInvalidRunnerIdentity
	}
	return store.runSQLiteTransaction(ctx, "runner identity binding", func(transaction *sql.Tx) error {
		return bindSQLiteRunnerIdentity(ctx, transaction, scaleSet, repository, identity, githubRunnerID, startedAt)
	})
}

func bindSQLiteRunnerIdentity(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	identity RunnerIdentity,
	githubRunnerID int64,
	startedAt time.Time,
) error {
	resolveErr := resolveSQLiteRunnerIdentity(ctx, transaction, scaleSet, repository, activeSQLiteRunner, identity)
	var err error
	if resolveErr == nil {
		err = updateSQLiteRunnerBusy(ctx, transaction, scaleSet, repository, identity, githubRunnerID, startedAt)
	} else if errors.Is(resolveErr, ErrRunnerIdentityMismatch) || errors.Is(resolveErr, ErrRunnerIdentityNotFound) {
		err = bindSQLiteIdentityFreeRunner(ctx, transaction, scaleSet, repository, identity, githubRunnerID, startedAt)
	} else {
		return resolveErr
	}
	if err != nil {
		return err
	}
	if err = bindSQLiteLibvirtMirror(ctx, transaction, scaleSet, repository, identity); err != nil {
		return err
	}
	return nil
}

func bindSQLiteIdentityFreeRunner(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	identity RunnerIdentity,
	githubRunnerID int64,
	startedAt time.Time,
) error {
	counts, err := sqliteBindingCandidateCounts(
		ctx, transaction, scaleSet, repository, identity,
	)
	if err != nil {
		return err
	}
	if err = classifySQLiteIdentityFreeBinding(identity, counts); err != nil {
		return err
	}
	result, err := transaction.ExecContext(ctx, `UPDATE runner_lifecycle
		SET phase = 'busy', request_id = ?, github_runner_id = CASE WHEN ? > 0 THEN ? ELSE github_runner_id END,
		updated_at = ? WHERE scale_set = ? AND repository = ? AND phase <> 'reap_pending'
		AND runner_name = ? AND request_id = 0`, identity.RequestID, githubRunnerID, githubRunnerID,
		startedAt.UnixNano(), scaleSet, repository, identity.RunnerName)
	if err != nil {
		return fmt.Errorf("bind runner identity: %w", err)
	}
	if err = requireSingleSQLiteIdentityChange(result, identity, "count bound runner identity"); err != nil {
		return err
	}
	return nil
}

func sqliteBindingCandidateCounts(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	identity RunnerIdentity,
) (sqliteBindingCounts, error) {
	requestMatches, err := countSQLiteRunnerMatches(ctx, transaction, activeSQLiteRunner,
		scaleSet, repository, RunnerIdentity{RequestID: identity.RequestID})
	if err != nil {
		return sqliteBindingCounts{}, err
	}
	nameMatches, err := countSQLiteRunnerMatches(ctx, transaction, activeSQLiteRunner,
		scaleSet, repository, RunnerIdentity{RunnerName: identity.RunnerName})
	if err != nil {
		return sqliteBindingCounts{}, err
	}
	identityFreeMatches, err := countSQLiteIdentityFreeRunnerMatches(
		ctx, transaction, scaleSet, repository, identity.RunnerName,
	)
	return sqliteBindingCounts{
		request: requestMatches, name: nameMatches, identityFree: identityFreeMatches,
	}, err
}

func classifySQLiteIdentityFreeBinding(
	identity RunnerIdentity,
	counts sqliteBindingCounts,
) error {
	if counts.request > 1 || counts.name > 1 || counts.identityFree > 1 {
		return &RunnerIdentityInvariantError{
			Identity: identity,
			Matches:  max(counts.request, counts.name, counts.identityFree),
		}
	}
	if counts.request > 0 || counts.name > 0 && counts.identityFree == 0 {
		return &RunnerIdentityMismatchError{Identity: identity}
	}
	if counts.identityFree == 0 {
		return &RunnerIdentityNotFoundError{Identity: identity}
	}
	return nil
}

func bindSQLiteLibvirtMirror(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	identity RunnerIdentity,
) error {
	counts, err := sqliteLibvirtBindingCandidateCounts(ctx, transaction, scaleSet, repository, identity)
	if err != nil {
		return err
	}
	shouldUpdate, err := classifySQLiteLibvirtBinding(identity, counts)
	if err != nil {
		return err
	}
	if !shouldUpdate {
		return nil
	}
	result, err := transaction.ExecContext(ctx, `UPDATE libvirt_runner_lifecycle SET request_id = ?
		WHERE scale_set = ? AND repository = ? AND runner_name = ? AND request_id = 0`,
		identity.RequestID, scaleSet, repository, identity.RunnerName)
	if err != nil {
		return fmt.Errorf("bind mirrored libvirt runner identity: %w", err)
	}
	return requireSingleSQLiteIdentityChange(result, identity, "count bound mirrored libvirt runner identity")
}

func sqliteLibvirtBindingCandidateCounts(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	identity RunnerIdentity,
) (sqliteLibvirtBindingCounts, error) {
	nameMatches, err := countSQLiteLibvirtMatches(ctx, transaction, anySQLiteRunner,
		scaleSet, repository, RunnerIdentity{RunnerName: identity.RunnerName})
	if err != nil || nameMatches == 0 {
		return sqliteLibvirtBindingCounts{name: nameMatches}, err
	}
	exactMatches, err := countSQLiteLibvirtMatches(ctx, transaction, anySQLiteRunner,
		scaleSet, repository, identity)
	if err != nil || exactMatches == 1 {
		return sqliteLibvirtBindingCounts{name: nameMatches, exact: exactMatches}, err
	}
	requestMatches, err := countSQLiteLibvirtMatches(ctx, transaction, anySQLiteRunner,
		scaleSet, repository, RunnerIdentity{RequestID: identity.RequestID})
	if err != nil {
		return sqliteLibvirtBindingCounts{}, err
	}
	identityFreeMatches, err := countSQLiteIdentityFreeLibvirtMatches(
		ctx, transaction, scaleSet, repository, identity.RunnerName,
	)
	return sqliteLibvirtBindingCounts{
		name: nameMatches, exact: exactMatches, request: requestMatches, identityFree: identityFreeMatches,
	}, err
}

func classifySQLiteLibvirtBinding(
	identity RunnerIdentity,
	counts sqliteLibvirtBindingCounts,
) (bool, error) {
	if counts.name == 0 || counts.exact == 1 {
		return false, nil
	}
	if counts.name > 1 {
		return false, &RunnerIdentityInvariantError{Identity: identity, Matches: counts.name}
	}
	if counts.request > 0 || counts.identityFree == 0 {
		return false, &RunnerIdentityMismatchError{Identity: identity}
	}
	return true, nil
}

func updateSQLiteRunnerBusy(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository string,
	identity RunnerIdentity,
	githubRunnerID int64,
	startedAt time.Time,
) error {
	query, identityArguments, err := sqliteIdentityStatement(identity,
		`UPDATE runner_lifecycle SET phase = 'busy', github_runner_id = CASE WHEN ? > 0 THEN ? ELSE github_runner_id END,
		updated_at = ? WHERE scale_set = ? AND repository = ? AND phase <> 'reap_pending' AND request_id = ?`,
		`UPDATE runner_lifecycle SET phase = 'busy', github_runner_id = CASE WHEN ? > 0 THEN ? ELSE github_runner_id END,
		updated_at = ? WHERE scale_set = ? AND repository = ? AND phase <> 'reap_pending' AND runner_name = ?`,
		`UPDATE runner_lifecycle SET phase = 'busy', github_runner_id = CASE WHEN ? > 0 THEN ? ELSE github_runner_id END,
		updated_at = ? WHERE scale_set = ? AND repository = ? AND phase <> 'reap_pending'
		AND request_id = ? AND runner_name = ?`)
	if err != nil {
		return err
	}
	arguments := []any{githubRunnerID, githubRunnerID, startedAt.UnixNano(), scaleSet, repository}
	result, err := transaction.ExecContext(ctx, query, append(arguments, identityArguments...)...)
	if err != nil {
		return fmt.Errorf("mark runner busy: %w", err)
	}
	return requireSingleSQLiteIdentityChange(result, identity, "count marked runner busy")
}

func countSQLiteIdentityFreeRunnerMatches(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository, runnerName string,
) (int64, error) {
	return countSQLiteFixedMatches(ctx, transaction, `SELECT COUNT(*) FROM runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND phase <> 'reap_pending'
		AND runner_name = ? AND request_id = 0`, "resolve identity-free runner", scaleSet, repository, runnerName)
}

func countSQLiteIdentityFreeLibvirtMatches(
	ctx context.Context,
	transaction *sql.Tx,
	scaleSet, repository, runnerName string,
) (int64, error) {
	return countSQLiteFixedMatches(ctx, transaction, `SELECT COUNT(*) FROM libvirt_runner_lifecycle
		WHERE scale_set = ? AND repository = ? AND runner_name = ? AND request_id = 0`,
		"resolve identity-free libvirt runner", scaleSet, repository, runnerName)
}

func countSQLiteFixedMatches(
	ctx context.Context,
	transaction *sql.Tx,
	query, operation string,
	arguments ...any,
) (int64, error) {
	var matches int64
	if err := transaction.QueryRowContext(ctx, query, arguments...).Scan(&matches); err != nil {
		return 0, fmt.Errorf("%s: %w", operation, err)
	}
	return matches, nil
}
