package daemon

import (
	"context"
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
	repair := leader.pendingRepositoryRepairs[repository.URL]
	repair.repository = repository
	repair.statuses = append([]cluster.ScaleSetStatus(nil), statuses...)
	delayIndex := min(repair.attempts, len(leader.repositoryRepairDelays)-1)
	delay := leader.repositoryRepairDelays[delayIndex]
	repair.attempts++
	repair.retryAfter = leader.now().Add(delay)
	leader.pendingRepositoryRepairs[repository.URL] = repair
	logAction(ctx, leader.daemon.logger, slog.LevelWarn, "repository_repair", "pending",
		slog.String("repository", repository.URL), slog.Int("attempt", repair.attempts),
		slog.Duration("retryAfter", delay), slog.String("error", repairErr.Error()))
}

func (leader *clusterLeader) completeRepositoryRepair(ctx context.Context, repository string) {
	repair, pending := leader.pendingRepositoryRepairs[repository]
	if !pending {
		return
	}
	delete(leader.pendingRepositoryRepairs, repository)
	logAction(ctx, leader.daemon.logger, slog.LevelInfo, "repository_repair", "completed",
		slog.String("repository", repository), slog.Int("attempt", repair.attempts+1))
}

func (leader *clusterLeader) retainDesiredRepositoryRepairs(
	repositories map[string]githubapp.Repository,
	desired map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus,
) {
	for repository := range leader.pendingRepositoryRepairs {
		_, repositoryDesired := repositories[repository]
		if !repositoryDesired || len(desiredRepositoryStatuses(repository, desired)) == 0 {
			delete(leader.pendingRepositoryRepairs, repository)
		}
	}
}

func (leader *clusterLeader) repairPendingRepositories(ctx context.Context) {
	leader.operationMutex.Lock()
	defer leader.operationMutex.Unlock()
	repairCtx, cancelRepair := context.WithCancel(ctx)
	if !leader.setOperationCancellation(cancelRepair) {
		cancelRepair()
		return
	}
	defer leader.clearOperationCancellation()
	defer cancelRepair()

	repositories := leader.dueRepositoryRepairs()
	for _, repository := range repositories {
		repair := leader.pendingRepositoryRepairs[repository]
		logAction(repairCtx, leader.daemon.logger, slog.LevelInfo, "repository_repair", "retrying",
			slog.String("repository", repository), slog.Int("attempt", repair.attempts+1))
		err := leader.reconcileDesiredRepository(repairCtx, repair.repository, repair.statuses)
		if err != nil {
			if repairCtx.Err() != nil {
				return
			}
			leader.recordRepositoryRepair(repairCtx, repair.repository, repair.statuses, err)
			continue
		}
		leader.completeRepositoryRepair(repairCtx, repository)
	}
}

func (leader *clusterLeader) dueRepositoryRepairs() []string {
	now := leader.now()
	repositories := make([]string, 0, len(leader.pendingRepositoryRepairs))
	for repository, repair := range leader.pendingRepositoryRepairs {
		if !now.Before(repair.retryAfter) {
			repositories = append(repositories, repository)
		}
	}
	sort.Strings(repositories)
	return repositories
}

func (leader *clusterLeader) nextRepositoryRepairDelay() (time.Duration, bool) {
	leader.operationMutex.Lock()
	defer leader.operationMutex.Unlock()
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
