package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	postgresstate "github.com/stateshifters/runnerfoundry/internal/state/postgres"
)

const deleteGitHubRunnerPoolDeclarationsSQL = `DELETE FROM public.runnerfoundry_github_member_runner_pools WHERE member_id = $1`
const insertGitHubRunnerPoolDeclarationSQL = `INSERT INTO public.runnerfoundry_github_member_runner_pools
	(member_id, runner_pool, runner_group) VALUES($1, $2, $3)`
const liveGitHubRunnerPoolDeclarationsSQL = `SELECT declarations.member_id, declarations.runner_pool, declarations.runner_group
	FROM public.runnerfoundry_github_member_runner_pools declarations
	JOIN public.runnerfoundry_members members ON members.member_id = declarations.member_id
	WHERE members.heartbeat_at >= $1
	ORDER BY declarations.member_id, declarations.runner_pool`

var _ githubsource.RunnerPoolDeclarationRepository = (*Store)(nil)

func (store *Store) ReplaceGitHubRunnerPoolDeclarations(
	ctx context.Context,
	memberID core.MemberID,
	declarations []githubsource.RunnerPoolDeclaration,
) (resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin GitHub Runner Pool declaration replacement: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	if _, err = transaction.Execute(ctx, deleteGitHubRunnerPoolDeclarationsSQL, memberID); err != nil {
		return fmt.Errorf("remove GitHub Runner Pool declarations: %w", err)
	}
	for _, declaration := range declarations {
		if _, err = transaction.Execute(ctx, insertGitHubRunnerPoolDeclarationSQL, memberID,
			declaration.RunnerPool, declaration.RunnerGroup); err != nil {
			return fmt.Errorf("insert GitHub Runner Pool declaration %q: %w", declaration.RunnerPool, err)
		}
	}
	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit GitHub Runner Pool declaration replacement: %w", err)
	}
	open = false
	return nil
}

func (store *Store) LiveGitHubRunnerPoolDeclarations(
	ctx context.Context,
	liveMemberCutoff time.Time,
) (_ []githubsource.MemberRunnerPoolDeclaration, resultErr error) {
	transaction, err := store.executor.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin live GitHub Runner Pool declaration lookup: %w", err)
	}
	open := true
	defer rollback(ctx, transaction, &open, &resultErr)
	rows, err := transaction.Query(ctx, liveGitHubRunnerPoolDeclarationsSQL, liveMemberCutoff)
	if err != nil {
		return nil, fmt.Errorf("query live GitHub Runner Pool declarations: %w", err)
	}
	declarations, err := scanGitHubRunnerPoolDeclarations(rows)
	if err != nil {
		return nil, err
	}
	if err = transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit live GitHub Runner Pool declaration lookup: %w", err)
	}
	open = false
	return declarations, nil
}

func scanGitHubRunnerPoolDeclarations(rows postgresstate.Rows) (declarations []githubsource.MemberRunnerPoolDeclaration, resultErr error) {
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		var memberID, runnerPool, runnerGroup string
		if err := rows.Scan(&memberID, &runnerPool, &runnerGroup); err != nil {
			return nil, fmt.Errorf("scan GitHub Runner Pool declaration: %w", err)
		}
		declarations = append(declarations, githubsource.MemberRunnerPoolDeclaration{
			MemberID: core.MemberID(memberID), RunnerPool: core.RunnerPool(runnerPool), RunnerGroup: runnerGroup,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate GitHub Runner Pool declarations: %w", err)
	}
	return declarations, nil
}
