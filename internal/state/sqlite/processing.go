package sqlite

import (
	"context"
	"fmt"
	"github.com/stateshifters/runnerfoundry/internal/core"
)

func (store *Store) ProcessingByMember(ctx context.Context, memberID core.MemberID, runnerPool core.RunnerPool) ([]core.ProcessingJob, error) {
	return store.processingJobs(ctx, processingByMemberSQL, string(memberID), string(runnerPool))
}
func (store *Store) ProcessingByPool(ctx context.Context, runnerPool core.RunnerPool) ([]core.ProcessingJob, error) {
	return store.processingJobs(ctx, processingByPoolSQL, string(runnerPool))
}
func (store *Store) processingJobs(ctx context.Context, statement string, arguments ...any) ([]core.ProcessingJob, error) {
	var jobs []core.ProcessingJob
	err := store.executor.WithinTransaction(ctx, func(transaction Transaction) error {
		var err error
		jobs, err = readClaimedProcessingJobs(ctx, transaction, statement, arguments...)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("load processing jobs: %w", err)
	}
	return jobs, nil
}
