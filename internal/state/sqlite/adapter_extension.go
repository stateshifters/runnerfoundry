package sqlite

import (
	"context"
	"fmt"
)

type AdapterExtension struct{ executor executor }

func (store *Store) AdapterExtension() *AdapterExtension {
	return &AdapterExtension{executor: store.executor}
}
func (extension *AdapterExtension) WithinTransaction(ctx context.Context, operation func(Transaction) error) error {
	if err := extension.executor.WithinTransaction(ctx, operation); err != nil {
		return fmt.Errorf("adapter persistence transaction: %w", err)
	}
	return nil
}
