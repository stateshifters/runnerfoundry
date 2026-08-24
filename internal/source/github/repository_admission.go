package github

import (
	"context"
	"fmt"
	"time"
)

const repositoryConcurrency = 4

func repositoryQuotas(appCount int) []int {
	quotas := make([]int, appCount)
	if appCount == 0 {
		return quotas
	}
	base := repositoryConcurrency / appCount
	if base < 1 {
		base = 1
	}
	for index := range quotas {
		quotas[index] = base
	}
	if appCount < repositoryConcurrency {
		for index := 0; index < repositoryConcurrency%appCount; index++ {
			quotas[index]++
		}
	}
	return quotas
}

func acquireRepositorySlot(ctx context.Context, slots chan struct{}, scope string) (func(), error) {
	if slots == nil {
		return func() {}, nil
	}
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("wait for %s repository reconciliation slot: %w", scope, ctx.Err())
	}
}

func repositoryOperationContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}
