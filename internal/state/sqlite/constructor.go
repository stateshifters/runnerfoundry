package sqlite

func NewStore(executor Executor) *Store { return &Store{executor: executor} }

func (store *Store) Close() error {
	if store.close == nil {
		return nil
	}
	return store.close()
}
