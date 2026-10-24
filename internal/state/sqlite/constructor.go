package sqlite

func newStore(executor executor) *Store { return &Store{executor: executor} }

func (store *Store) Close() error {
	if store.close == nil {
		return nil
	}
	return store.close()
}
