package sqlite

import (
	"context"
	"errors"
	"fmt"

	"github.com/stateshifters/runnerfoundry/internal/core"
)

func transactionBeginning(transaction transactionStub) func(context.Context) (transactionStub, error) {
	return func(context.Context) (transactionStub, error) { return transaction, nil }
}

func successfulExecute(context.Context, string, ...any) (resultStub, error) {
	return successfulResult(), nil
}

func unstartedProcessingValues() []any {
	values := claimedProcessingJobValues()
	values[8], values[9] = false, false
	return values
}

func retryProcessingJob() core.ProcessingJob {
	return core.ProcessingJob{
		Job: core.Job{
			ID:         testJobID,
			Source:     core.SourceReference{Adapter: "github", ID: "request-3"},
			RunnerPool: "linux",
		},
		MemberID:            testMemberID,
		RunnerName:          core.AttemptRunnerName(testJobID, core.AttemptID{1}),
		ClaimedAt:           fixedTime(),
		InventoryEndpointID: "test:endpoint",
	}
}

func successfulCompletion(context.Context) error { return nil }

func sqliteClaimedValues(processing core.ProcessingJob, accepted, terminal bool) []any {
	return []any{processing.Job.ID.String(), string(processing.Job.Source.Adapter), string(processing.Job.Source.ID),
		string(processing.Job.RunnerPool), string(processing.MemberID), string(processing.RunnerName), processing.ClaimedAt.UnixNano(),
		string(processing.InventoryEndpointID), accepted, terminal, ""}
}

func validProcessingClaim() core.ProcessingClaim {
	return core.ProcessingClaim{MemberID: testMemberID, AttemptID: core.AttemptID{1},
		ClaimedAt: fixedTime(), InventoryEndpointID: "test:endpoint"}
}

func assignDestinations(destinations, values []any) error {
	if len(destinations) != len(values) {
		return errors.New("destination count")
	}
	for index, destination := range destinations {
		if !assignSQLiteTestValue(destination, values[index]) {
			return fmt.Errorf("cannot assign %T to %T", values[index], destination)
		}
	}
	return nil
}

func assignSQLiteTestValue(destination, value any) bool {
	switch target := destination.(type) {
	case *string:
		assigned, ok := value.(string)
		*target = assigned
		return ok
	case *int:
		assigned, ok := value.(int)
		*target = assigned
		return ok
	case *int64:
		assigned, ok := value.(int64)
		*target = assigned
		return ok
	case *bool:
		assigned, ok := value.(bool)
		*target = assigned
		return ok
	default:
		return false
	}
}

func failingListTransactions(protocolErr error) []transactionStub {
	return []transactionStub{
		{query: func(context.Context, string, ...any) (rowsStub, error) { return rowsStub{}, protocolErr }, rollback: successfulCompletion},
		{query: func(context.Context, string, ...any) (rowsStub, error) {
			return rowsWithFailure(protocolErr, nil, nil), nil
		}, rollback: successfulCompletion},
		{query: func(context.Context, string, ...any) (rowsStub, error) {
			return rowsWithFailure(nil, protocolErr, nil), nil
		}, rollback: successfulCompletion},
		{query: func(context.Context, string, ...any) (rowsStub, error) {
			return rowsWithFailure(nil, nil, protocolErr), nil
		}, rollback: successfulCompletion},
		{query: func(context.Context, string, ...any) (rowsStub, error) { return rowsFromValues(nil), nil },
			commit: failingCompletion(protocolErr), rollback: successfulCompletion},
	}
}

type namedTransaction struct {
	name        string
	transaction transactionStub
}

func namedFailingListTransactions(protocolErr error) []namedTransaction {
	names := []string{"query", "row scan", "iteration", "close", "commit"}
	transactions := failingListTransactions(protocolErr)
	result := make([]namedTransaction, len(transactions))
	for index := range transactions {
		result[index] = namedTransaction{name: names[index], transaction: transactions[index]}
	}
	return result
}
