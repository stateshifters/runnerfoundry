package sqlite

import "context"

type Scanner interface {
	Scan(destinations ...any) error
}
type Transaction interface {
	Execute(ctx context.Context, statement string, arguments ...any) (int64, error)
	ReadRow(ctx context.Context, statement string, read func(Scanner) error, arguments ...any) error
	VisitRows(ctx context.Context, statement string, visit func(Scanner) error, arguments ...any) error
}
type executor interface {
	WithinTransaction(ctx context.Context, operation func(Transaction) error) error
	ReadRow(ctx context.Context, statement string, read func(Scanner) error, arguments ...any) error
}
