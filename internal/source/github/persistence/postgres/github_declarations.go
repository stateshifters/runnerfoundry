package postgres

import (
	"context"
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
	WHERE declarations.member_id = ANY($1)
	ORDER BY declarations.member_id, declarations.runner_pool`

var _ githubsource.RunnerPoolDeclarationRepository = (*Store)(nil)

func (store *Store) ReplaceGitHubRunnerPoolDeclarations(
	ctx context.Context,
	memberID core.MemberID,
	declarations []githubsource.RunnerPoolDeclaration,
) error {
	if err := store.transactions.WithinTransaction(ctx, func(transaction postgresstate.Transaction) error {
		if _, err := transaction.Execute(ctx, deleteGitHubRunnerPoolDeclarationsSQL, memberID); err != nil {
			return fmt.Errorf("remove GitHub Runner Pool declarations: %w", err)
		}
		for _, declaration := range declarations {
			if _, err := transaction.Execute(ctx, insertGitHubRunnerPoolDeclarationSQL, memberID,
				declaration.RunnerPool, declaration.RunnerGroup); err != nil {
				return fmt.Errorf("insert GitHub Runner Pool declaration %q: %w", declaration.RunnerPool, err)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("replace GitHub Runner Pool declarations transaction: %w", err)
	}
	return nil
}

func (store *Store) LiveGitHubRunnerPoolDeclarations(
	ctx context.Context,
	liveMemberCutoff time.Time,
) ([]githubsource.MemberRunnerPoolDeclaration, error) {
	liveMemberIDs, err := store.memberLiveness.LiveMemberIDs(ctx, liveMemberCutoff)
	if err != nil {
		return nil, fmt.Errorf("read live members for GitHub Runner Pool declarations: %w", err)
	}
	liveMembers := make([]string, len(liveMemberIDs))
	for index, memberID := range liveMemberIDs {
		liveMembers[index] = string(memberID)
	}
	var declarations []githubsource.MemberRunnerPoolDeclaration
	if err = store.transactions.WithinTransaction(ctx, func(transaction postgresstate.Transaction) error {
		return transaction.VisitRows(ctx, liveGitHubRunnerPoolDeclarationsSQL, func(row postgresstate.Scanner) error {
			declaration, scanErr := scanGitHubRunnerPoolDeclaration(row)
			if scanErr != nil {
				return scanErr
			}
			declarations = append(declarations, declaration)
			return nil
		}, liveMembers)
	}); err != nil {
		return nil, fmt.Errorf("live GitHub Runner Pool declaration lookup: %w", err)
	}
	return declarations, nil
}

func scanGitHubRunnerPoolDeclaration(row postgresstate.Scanner) (githubsource.MemberRunnerPoolDeclaration, error) {
	var memberID, runnerPool, runnerGroup string
	if err := row.Scan(&memberID, &runnerPool, &runnerGroup); err != nil {
		return githubsource.MemberRunnerPoolDeclaration{}, fmt.Errorf("scan GitHub Runner Pool declaration: %w", err)
	}
	return githubsource.MemberRunnerPoolDeclaration{
		MemberID: core.MemberID(memberID), RunnerPool: core.RunnerPool(runnerPool), RunnerGroup: runnerGroup,
	}, nil
}
