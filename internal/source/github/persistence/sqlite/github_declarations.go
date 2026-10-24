package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/stateshifters/runnerfoundry/internal/core"
	githubsource "github.com/stateshifters/runnerfoundry/internal/source/github"
	sqlitestate "github.com/stateshifters/runnerfoundry/internal/state/sqlite"
)

const deleteGitHubRunnerPoolDeclarationsSQL = `DELETE FROM runnerfoundry_github_member_runner_pools WHERE member_id = ?`
const insertGitHubRunnerPoolDeclarationSQL = `INSERT INTO runnerfoundry_github_member_runner_pools
	(member_id, runner_pool, runner_group) VALUES(?, ?, ?)`
const liveGitHubRunnerPoolDeclarationsSQL = `SELECT declarations.member_id, declarations.runner_pool, declarations.runner_group
	FROM runnerfoundry_github_member_runner_pools declarations
	ORDER BY declarations.member_id, declarations.runner_pool`

var _ githubsource.RunnerPoolDeclarationRepository = (*Store)(nil)

func (store *Store) ReplaceGitHubRunnerPoolDeclarations(
	ctx context.Context,
	memberID core.MemberID,
	declarations []githubsource.RunnerPoolDeclaration,
) error {
	if err := store.transactions.WithinTransaction(ctx, func(transaction sqlitestate.Transaction) error {
		if _, err := transaction.Execute(ctx, deleteGitHubRunnerPoolDeclarationsSQL, string(memberID)); err != nil {
			return fmt.Errorf("remove GitHub Runner Pool declarations: %w", err)
		}
		for _, declaration := range declarations {
			if _, err := transaction.Execute(ctx, insertGitHubRunnerPoolDeclarationSQL, string(memberID),
				string(declaration.RunnerPool), declaration.RunnerGroup); err != nil {
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
	liveMembers := make(map[core.MemberID]struct{}, len(liveMemberIDs))
	for _, memberID := range liveMemberIDs {
		liveMembers[memberID] = struct{}{}
	}
	var declarations []githubsource.MemberRunnerPoolDeclaration
	if err = store.transactions.WithinTransaction(ctx, func(transaction sqlitestate.Transaction) error {
		return transaction.VisitRows(ctx, liveGitHubRunnerPoolDeclarationsSQL, func(row sqlitestate.Scanner) error {
			declaration, live, scanErr := scanGitHubRunnerPoolDeclaration(row, liveMembers)
			if scanErr != nil {
				return scanErr
			}
			if live {
				declarations = append(declarations, declaration)
			}
			return nil
		})
	}); err != nil {
		return nil, fmt.Errorf("live GitHub Runner Pool declaration lookup: %w", err)
	}
	return declarations, nil
}

func scanGitHubRunnerPoolDeclaration(
	row sqlitestate.Scanner,
	liveMembers map[core.MemberID]struct{},
) (githubsource.MemberRunnerPoolDeclaration, bool, error) {
	var memberID, runnerPool, runnerGroup string
	if err := row.Scan(&memberID, &runnerPool, &runnerGroup); err != nil {
		return githubsource.MemberRunnerPoolDeclaration{}, false,
			fmt.Errorf("scan GitHub Runner Pool declaration: %w", err)
	}
	_, live := liveMembers[core.MemberID(memberID)]
	return githubsource.MemberRunnerPoolDeclaration{
		MemberID: core.MemberID(memberID), RunnerPool: core.RunnerPool(runnerPool), RunnerGroup: runnerGroup,
	}, live, nil
}
