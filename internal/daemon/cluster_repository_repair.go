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

var errRepositorySessionGenerationIncomplete = errors.New("repository session generation is incomplete")

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
	generation uint64
	attempts   int
	retryAfter time.Time
}

type repositoryRepairResult struct {
	repair repositoryRepair
	err    error
}

func (leader *clusterLeader) reconcileDesiredRepository(
	ctx context.Context,
	desired desiredRepositoryGeneration,
) error {
	repairCtx, cancelRepair := context.WithTimeout(ctx, leader.githubOperationTimeout)
	defer cancelRepair()
	return leader.reconcileRepositoryGeneration(repairCtx, desired)
}

func (leader *clusterLeader) recordDesiredRepositoryRepair(
	ctx context.Context,
	desired desiredRepositoryGeneration,
	repairErr error,
) {
	repository := desired.repository
	statuses := desired.statuses
	leader.repairMutex.Lock()
	repair := leader.pendingRepositoryRepairs[repository.URL]
	if repair.repository == repository && sameScaleSetStatuses(repair.statuses, statuses) &&
		repair.generation == desired.generation && leader.now().Before(repair.retryAfter) {
		leader.repairMutex.Unlock()
		return
	}
	repair.repository = repository
	repair.statuses = append([]cluster.ScaleSetStatus(nil), statuses...)
	repair.generation = desired.generation
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

func (leader *clusterLeader) repositoryRepairAlreadyScheduled(
	repository githubapp.Repository,
	statuses []cluster.ScaleSetStatus,
) bool {
	desired, _ := leader.currentDesiredRepository(repository.URL)
	leader.repairMutex.Lock()
	defer leader.repairMutex.Unlock()
	repair, pending := leader.pendingRepositoryRepairs[repository.URL]
	if !pending {
		return false
	}
	if repair.repository == repository && sameScaleSetStatuses(repair.statuses, statuses) &&
		repair.generation == desired.generation {
		return true
	}
	return false
}

func requestRepositoryRepair(requests chan<- struct{}) {
	select {
	case requests <- struct{}{}:
	default:
	}
}

func (leader *clusterLeader) completeRepositoryRepair(ctx context.Context, repository string, generation uint64) {
	leader.repairMutex.Lock()
	repair, pending := leader.pendingRepositoryRepairs[repository]
	if !pending || repair.generation != generation {
		leader.repairMutex.Unlock()
		return
	}
	delete(leader.pendingRepositoryRepairs, repository)
	leader.repairMutex.Unlock()
	logAction(ctx, leader.daemon.logger, slog.LevelInfo, "repository_repair", "completed",
		slog.String("repository", repository), slog.Int("attempt", repair.attempts+1))
}

func (leader *clusterLeader) cancelRepositoryRepair(ctx context.Context, repository string, generation uint64) {
	leader.repairMutex.Lock()
	repair, pending := leader.pendingRepositoryRepairs[repository]
	if !pending || repair.generation != generation {
		leader.repairMutex.Unlock()
		return
	}
	delete(leader.pendingRepositoryRepairs, repository)
	leader.repairMutex.Unlock()
	logAction(ctx, leader.daemon.logger, slog.LevelInfo, "repository_repair", "canceled",
		slog.String("repository", repository), slog.Int("attempt", repair.attempts))
}

func (leader *clusterLeader) supersedeRepositoryRepair(
	ctx context.Context,
	desired desiredRepositoryGeneration,
) {
	leader.repairMutex.Lock()
	repair := leader.pendingRepositoryRepairs[desired.repository.URL]
	repair.repository = desired.repository
	repair.statuses = append([]cluster.ScaleSetStatus(nil), desired.statuses...)
	repair.generation = desired.generation
	repair.retryAfter = leader.now()
	leader.pendingRepositoryRepairs[desired.repository.URL] = repair
	leader.repairMutex.Unlock()
	logAction(ctx, leader.daemon.logger, slog.LevelInfo, "repository_repair", "superseded",
		slog.String("repository", desired.repository.URL), slog.Int("attempt", repair.attempts))
	requestRepositoryRepair(leader.daemon.repositoryRepairRequests)
}

func (leader *clusterLeader) retainDesiredRepositoryRepairs(
	repositories map[string]githubapp.Repository,
	desired map[repositoryScaleSetSessionKey]cluster.ScaleSetStatus,
) {
	desiredRepairs := make(map[string]desiredRepositoryGeneration, len(repositories))
	for repository := range repositories {
		current, exists := leader.currentDesiredRepository(repository)
		if exists {
			desiredRepairs[repository] = current
		}
	}
	leader.repairMutex.Lock()
	defer leader.repairMutex.Unlock()
	for repository, repair := range leader.pendingRepositoryRepairs {
		_, repositoryDesired := repositories[repository]
		current, generationDesired := desiredRepairs[repository]
		if !repositoryDesired || len(desiredRepositoryStatuses(repository, desired)) == 0 || !generationDesired {
			delete(leader.pendingRepositoryRepairs, repository)
			continue
		}
		if repair.repository != current.repository || !sameScaleSetStatuses(repair.statuses, current.statuses) ||
			repair.generation != current.generation {
			repair.repository = current.repository
			repair.statuses = append([]cluster.ScaleSetStatus(nil), current.statuses...)
			repair.generation = current.generation
			repair.retryAfter = leader.now()
			leader.pendingRepositoryRepairs[repository] = repair
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
			return leader.reconcileDesiredRepository(attemptCtx, desiredRepositoryGeneration{
				repository: repair.repository,
				statuses:   repair.statuses,
				generation: repair.generation,
			})
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
	leader.finishRepositoryReconciliation(ctx, desiredRepositoryGeneration{
		repository: result.repair.repository,
		statuses:   result.repair.statuses,
		generation: result.repair.generation,
	}, result.err)
}

func (leader *clusterLeader) finishRepositoryReconciliation(
	ctx context.Context,
	expected desiredRepositoryGeneration,
	resultErr error,
) {
	authority := leader.repositoryReconciliationAuthority(expected)
	if authority.removed {
		leader.cancelRepositoryRepair(ctx, expected.repository.URL, expected.generation)
		return
	}
	if authority.superseded || errors.Is(resultErr, errRepositoryWorkObsolete) {
		leader.supersedeRepositoryRepair(ctx, authority.desired)
		return
	}
	leader.finishAuthorizedRepositoryReconciliation(ctx, authority, resultErr)
}

type repositoryReconciliationAuthority struct {
	desired    desiredRepositoryGeneration
	known      bool
	removed    bool
	superseded bool
}

func (leader *clusterLeader) repositoryReconciliationAuthority(
	expected desiredRepositoryGeneration,
) repositoryReconciliationAuthority {
	desired, exists, known := leader.desiredRepositorySnapshot(expected.repository.URL)
	if known && !exists {
		return repositoryReconciliationAuthority{known: true, removed: true}
	}
	if !exists {
		desired = expected
	}
	superseded := desired.generation != expected.generation || desired.repository != expected.repository ||
		!sameScaleSetStatuses(desired.statuses, expected.statuses)
	return repositoryReconciliationAuthority{desired: desired, known: known, superseded: superseded}
}

func (leader *clusterLeader) finishAuthorizedRepositoryReconciliation(
	ctx context.Context,
	authority repositoryReconciliationAuthority,
	resultErr error,
) {
	desired := authority.desired
	if resultErr != nil {
		leader.recordDesiredRepositoryRepair(ctx, desired, resultErr)
		return
	}
	if leader.repositoryGenerationReady(desired) {
		leader.completeRepositoryRepair(ctx, desired.repository.URL, desired.generation)
		return
	}
	if !authority.known && leader.repositoryRepairSupersedes(desired) {
		return
	}
	leader.recordDesiredRepositoryRepair(ctx, desired, errRepositorySessionGenerationIncomplete)
}

func (leader *clusterLeader) repositoryRepairSupersedes(desired desiredRepositoryGeneration) bool {
	leader.repairMutex.Lock()
	defer leader.repairMutex.Unlock()
	repair, exists := leader.pendingRepositoryRepairs[desired.repository.URL]
	return exists && repair.generation > desired.generation
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
