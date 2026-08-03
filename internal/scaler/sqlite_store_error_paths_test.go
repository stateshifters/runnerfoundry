package scaler

import (
	"context"
	"errors"
	"testing"
)

func TestSQLiteStoreErrorPathsStayInMemory(t *testing.T) {
	failure := errors.New("failed")
	fixture := mockDemandStore(t)
	fixture.mock.ExpectBegin().WillReturnError(failure)
	if err := fixture.store.Apply(context.Background(), "set", nil, nil); err == nil {
		t.Fatal("expected begin error")
	}
	for _, operation := range []func() error{
		func() error { return fixture.store.RemoveRepository(context.Background(), "set", "repo") },
		func() error { return fixture.store.RemoveScaleSet(context.Background(), "set") },
	} {
		fixture.mock.ExpectBegin().WillReturnError(failure)
		if err := operation(); err == nil {
			t.Fatal("expected mutation error")
		}
	}
	fixture.mock.ExpectExec("DELETE FROM demand").WillReturnError(failure)
	if err := fixture.store.Delete(context.Background(), "set", "repo", 1); err == nil {
		t.Fatal("expected mutation error")
	}
	fixture.mock.ExpectQuery("SELECT demand_id").WillReturnError(failure)
	if _, err := fixture.store.Ordered(context.Background(), "set"); err == nil {
		t.Fatal("expected query error")
	}
}

func TestSQLiteStoreMigrationAndApplyFailuresStayInMemory(t *testing.T) {
	failure := errors.New("failed")
	for _, observed := range []bool{true, false} {
		fixture := mockDemandStore(t)
		fixture.mock.ExpectBegin()
		fixture.mock.ExpectExec(".*").WillReturnError(failure)
		fixture.mock.ExpectRollback()
		var observedDemand []JobDemand
		var removals []DemandRemoval
		if observed {
			observedDemand = []JobDemand{{Repository: "repo", RequestID: 1}}
		} else {
			removals = []DemandRemoval{{Repository: "repo", RequestID: 1}}
		}
		if err := fixture.store.Apply(context.Background(), "set", observedDemand, removals); err == nil {
			t.Fatal("expected apply error")
		}
	}
	fixture := mockDemandStore(t)
	expectSchemaVersion(fixture.mock, 0)
	fixture.mock.ExpectBegin()
	fixture.mock.ExpectExec("CREATE TABLE demand").WillReturnError(failure)
	fixture.mock.ExpectRollback()
	if err := fixture.store.migrate(context.Background()); err == nil {
		t.Fatal("expected migration error")
	}
}
