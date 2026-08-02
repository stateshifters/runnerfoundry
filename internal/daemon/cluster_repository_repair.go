package daemon

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"time"

	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/githubapp"
)

var repositoryRepairRetryDelays = [...]time.Duration{
	5 * time.Second,
	10 * time.Second,
	20 * time.Second,
	40 * time.Second,
	60 * time.Second,
}

type repositoryRepair struct {
	repository githubapp.Repository
	statuses   []cluster.ScaleSetStatus
	attempts   int
	retryAfter time.Time
}

type repositoryRepairResult struct {
	repair repositoryRepair
	err    error
}

func (leader *clusterLeader) reconcileDesiredRepository(
	ctx context.Context,
	repository githubapp.Repository,
	statuses []cluster.ScaleSetStatus,
) error {
	repairCtx, cancelRepair := context.WithTimeout(ctx, leader.githubOperationTimeout)
	defer cancelRepair()
	return leader.reconcileRepositorySessions(repairCtx, repository, statuses)
}

func (leader *clusterLeader) recordRepositoryRepair(
	ctx context.Context,
	repository githubapp.Repository,
	statuses []cluster.ScaleSetStatus,
	repairErr error,
) {
	leader.repairMutex.Lock()
	repair := leader.pendingRepositoryRepairs[repository.URL]
	repair.repository = repository
	repair.statuses = append([]cluster.ScaleSetStatus(nil), statuses...)
	delayIndex := min(repair.attempts, len(leader.repositoryRepairDelays)-1)
	delay := leader.repositoryRepairDelays[delayIndex]
	repair.attempts++
	repair.retryAfter = leader.now().Add(delay)
	leader.pendingRepositoryRepairs[repository.URL] = repair
	leader.repairMutex.Unlock()
	logAction(ctx, leader.daemon.logger, slog.LevelWarn, "repository_repair", "pending",
		slog.String("repository", repository.URL), slog.Int("attempt", repair.attempts),
		slog.Duration("retryAfter", delay), slog.String("error", repairErr.Error()))
	requestRepositoryRepair(leader.daemon.repositoryRepairRequests)
}

func requestRepositoryRepair(requests chan<- struct{}) {
	select {
	case requests <- struct{}{}:
	default:
	}
}

func (leader *clusterLeader) completeRepositoryRepair(ctx context.Context, repository string) {
	leader.repairMutex.Lock()
	repair, pending := leader.pendingRepositoryRepairs[repository]
	if !pending {
		leader.repairMutex.Unlock()
		return
	}
	delete(leader.pendingRepositoryRepairs, repository)
	leader.repairMutex.Unlock()
	logAction(ctx, leader.daemon.logger, slog.LevelInfo, "repository_repair", "completed",
		slog.String("repository", repository), slog.Int("attempt", repair.attempts+1))
}

func (leader *clusterLeader) retainDesiredRepositoryRepairs(
	repositories map[string]githubapp.Repository,
	desired map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus,
) {
	leader.repairMutex.Lock()
	defer leader.repairMutex.Unlock()
	for repository := range leader.pendingRepositoryRepairs {
		_, repositoryDesired := repositories[repository]
		if !repositoryDesired || len(desiredRepositoryStatuses(repository, desired)) == 0 {
			delete(leader.pendingRepositoryRepairs, repository)
		}
	}
}

func (leader *clusterLeader) repairPendingRepositories(ctx context.Context) {
	if leader.repositoryRepairStopped() {
		return
	}
	repairs := leader.dueRepositoryRepairs()
	results := leader.enqueueRepositoryRepairs(ctx, repairs)
	leader.awaitRepositoryRepairs(ctx, len(repairs), results)
}

func (leader *clusterLeader) repositoryRepairStopped() bool {
	leader.cancelMutex.Lock()
	defer leader.cancelMutex.Unlock()
	return leader.stopped
}

func (leader *clusterLeader) enqueueRepositoryRepairs(
	ctx context.Context,
	repairs []repositoryRepair,
) <-chan repositoryRepairResult {
	results := make(chan repositoryRepairResult, len(repairs))
	for _, repair := range repairs {
		repository := repair.repository.URL
		logAction(ctx, leader.daemon.logger, slog.LevelInfo, "repository_repair", "retrying",
			slog.String("repository", repository), slog.Int("attempt", repair.attempts+1))
		leader.repositoryWorkers.enqueue(repository, repositoryWorkUrgent, func(attemptCtx context.Context) error {
			return leader.reconcileDesiredRepository(attemptCtx, repair.repository, repair.statuses)
		}, func(err error) { results <- repositoryRepairResult{repair: repair, err: err} })
	}
	return results
}

func (leader *clusterLeader) awaitRepositoryRepairs(
	ctx context.Context,
	expected int,
	results <-chan repositoryRepairResult,
) {
	for completed := 0; completed < expected; completed++ {
		var result repositoryRepairResult
		select {
		case <-ctx.Done():
			return
		case <-leader.repositoryWorkerDone:
			return
		case result = <-results:
		}
		leader.applyRepositoryRepairResult(ctx, result)
	}
}

func (leader *clusterLeader) applyRepositoryRepairResult(ctx context.Context, result repositoryRepairResult) {
	if result.err != nil && !errors.Is(result.err, errRepositoryWorkObsolete) {
		leader.recordRepositoryRepair(ctx, result.repair.repository, result.repair.statuses, result.err)
		return
	}
	leader.completeRepositoryRepair(ctx, result.repair.repository.URL)
}

func (leader *clusterLeader) dueRepositoryRepairs() []repositoryRepair {
	leader.repairMutex.Lock()
	defer leader.repairMutex.Unlock()
	now := leader.now()
	repairs := make([]repositoryRepair, 0, len(leader.pendingRepositoryRepairs))
	for _, repair := range leader.pendingRepositoryRepairs {
		if !now.Before(repair.retryAfter) {
			repairs = append(repairs, repair)
		}
	}
	sort.Slice(repairs, func(left, right int) bool {
		return repairs[left].repository.URL < repairs[right].repository.URL
	})
	return repairs
}

func (leader *clusterLeader) nextRepositoryRepairDelay() (time.Duration, bool) {
	leader.repairMutex.Lock()
	defer leader.repairMutex.Unlock()
	if len(leader.pendingRepositoryRepairs) == 0 {
		return 0, false
	}
	now := leader.now()
	var earliest time.Time
	for _, repair := range leader.pendingRepositoryRepairs {
		if earliest.IsZero() || repair.retryAfter.Before(earliest) {
			earliest = repair.retryAfter
		}
	}
	return max(earliest.Sub(now), time.Duration(0)), true
}
