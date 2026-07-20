package scaler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteDemandStorePreservesFIFOAcrossReopen(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	queueTime := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	if err := store.Apply(context.Background(), "linux", []JobDemand{
		{Repository: "newer", RequestID: 2, QueueTime: queueTime.Add(time.Minute)},
		{Repository: "older", RequestID: 1, QueueTime: queueTime},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	ordered, err := reopened.Ordered(context.Background(), "linux")
	if err != nil {
		t.Fatal(err)
	}
	if len(ordered) != 2 || ordered[0].RequestID != 1 || ordered[1].RequestID != 2 {
		t.Fatalf("expected persisted FIFO demand, got %#v", ordered)
	}
}

func TestSQLiteDemandStoreReopensTwoHundredQueuedJobs(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	queueTime := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	observed := make([]JobDemand, 200)
	for index := range observed {
		observed[index] = JobDemand{Repository: "repo", RequestID: int64(200 - index), QueueTime: queueTime.Add(time.Duration(200-index) * time.Second)}
	}
	if err := store.Apply(context.Background(), "linux", observed, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLiteDemandStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	ordered, err := reopened.Ordered(context.Background(), "linux")
	if err != nil {
		t.Fatal(err)
	}
	if len(ordered) != 200 || ordered[0].RequestID != 1 || ordered[199].RequestID != 200 {
		t.Fatalf("unexpected recovered queue boundaries: %d %#v %#v", len(ordered), ordered[0], ordered[len(ordered)-1])
	}
}

func TestSQLiteDemandStoreRejectsSecondOwner(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	first, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer first.Close()
	if _, err := OpenSQLiteDemandStore(databasePath); err == nil {
		t.Fatal("expected second state owner to be rejected")
	}
}

func TestSQLiteDemandStoreMutationsAreDurableAndIdempotent(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "state.db")
	store, openErr := OpenSQLiteDemandStore(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer store.Close()
	ctx := context.Background()
	demand := JobDemand{Repository: "repo", RequestID: 7, QueueTime: time.Now()}
	if err := store.Apply(ctx, "linux", []JobDemand{demand, demand}, nil); err != nil {
		t.Fatal(err)
	}
	demand.Acquired = true
	if err := store.Apply(ctx, "linux", []JobDemand{demand}, nil); err != nil {
		t.Fatal(err)
	}
	ordered, err := store.Ordered(ctx, "linux")
	if err != nil || len(ordered) != 1 || !ordered[0].Acquired {
		t.Fatalf("expected acquired deduplicated demand, got %#v, %v", ordered, err)
	}
	if err := store.Delete(ctx, "linux", "repo", 7); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(ctx, "linux", []JobDemand{demand}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveRepository(ctx, "linux", "repo"); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(ctx, "linux", []JobDemand{demand}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveScaleSet(ctx, "linux"); err != nil {
		t.Fatal(err)
	}
}
