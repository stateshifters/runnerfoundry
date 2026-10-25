package libvirt

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestInventoryPublishesCompleteDefensiveSnapshotsDuringConcurrentRefresh(t *testing.T) {
	provider := NewProvider(&concurrentDomainReader{}, Config{ConnectionURI: "qemu:///system"}, nil, "member")
	if err := provider.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() { exerciseConcurrentLibvirtSnapshots(t, provider) })
	}
	workers.Wait()
}

func exerciseConcurrentLibvirtSnapshots(t *testing.T, provider Provider) {
	t.Helper()
	for range 20 {
		if err := provider.Refresh(context.Background()); err != nil {
			t.Error(err)
			return
		}
		instances, err := provider.Inspect(context.Background())
		if err != nil || len(instances) != 2 {
			t.Errorf("incomplete snapshot=%+v error=%v", instances, err)
			return
		}
		if instances[0].RunnerPool != instances[1].RunnerPool {
			t.Errorf("mixed snapshot=%+v", instances)
			return
		}
		instances[0].RunnerPool = "changed-by-caller"
	}
}

type concurrentDomainReader struct {
	CommandRunner
	version atomic.Int64
}

func (commands *concurrentDomainReader) ListDomains(context.Context, string) (string, error) {
	version := commands.version.Add(1)
	return fmt.Sprintf("first-%d\nsecond-%d", version, version), nil
}

func (*concurrentDomainReader) ReadDomainXML(_ context.Context, _ string, name string) (string, error) {
	_, version, _ := strings.Cut(name, "-")
	return fmt.Sprintf(`<domain><metadata><runnerfoundry:runner xmlns:runnerfoundry="urn:runnerfoundry"><runnerfoundry:runner-pool>pool-%s</runnerfoundry:runner-pool></runnerfoundry:runner></metadata></domain>`, version), nil
}

func (*concurrentDomainReader) ReadDomainState(context.Context, string, string) (string, error) {
	return "running", nil
}
