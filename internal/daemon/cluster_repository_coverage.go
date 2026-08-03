package daemon

import (
	"context"
	"log/slog"

	upstream "github.com/actions/scaleset"
	"github.com/meza/RunnerFoundry/internal/cluster"
	"github.com/meza/RunnerFoundry/internal/githubapp"
	"github.com/meza/RunnerFoundry/internal/repositorystate"
)

func (leader *clusterLeader) repositorySessionsMatch(
	repository string,
	statuses []cluster.ScaleSetStatus,
	registrations map[string]*upstream.RunnerScaleSet,
) bool {
	leader.sessionsMutex.Lock()
	defer leader.sessionsMutex.Unlock()
	if activeRepositorySessionCount(leader.sessions, repository) != len(statuses) {
		return false
	}
	for _, status := range statuses {
		session := leader.sessions[repositoryScaleSetSessionKey{repository: repository, scaleSet: status.Name}]
		registration := registrations[status.Name]
		if !repositoryAuditedSessionMatches(session, registration, status, leader.repositoryControllerEpoch) {
			return false
		}
	}
	return true
}

func repositoryAuditedSessionMatches(
	session *repositoryScaleSetSession,
	registration *upstream.RunnerScaleSet,
	status cluster.ScaleSetStatus,
	controllerEpoch int64,
) bool {
	return session != nil && session.runnerScaleSet != nil && registration != nil &&
		session.runnerScaleSet.ID == registration.ID && session.capacity == status.EffectiveCapacity &&
		session.assignedJobs != nil && session.assignedJobs.controllerEpoch == controllerEpoch &&
		session.assignedJobs.latest() != nil && sessionPollerRunning(session.pollerDone)
}

type repositorySessionCoverage struct {
	desired              int
	active               int
	ready                int
	currentEpochObserved int
	pendingRepair        int
	missingScaleSets     []string
}

func (coverage repositorySessionCoverage) complete() bool {
	return coverage.active == coverage.desired && coverage.ready == coverage.desired &&
		coverage.currentEpochObserved == coverage.desired
}

func (leader *clusterLeader) loadRepositorySessionCoverage(
	desired desiredRepositoryGeneration,
) repositorySessionCoverage {
	registrations := leader.confirmedRegistrationSnapshot()
	leader.sessionsMutex.Lock()
	coverage := repositorySessionCoverage{
		desired: len(desired.statuses), missingScaleSets: make([]string, 0, len(desired.statuses)),
	}
	coverage.active = activeRepositorySessionCount(leader.sessions, desired.repository.URL)
	leader.addDesiredSessionCoverage(&coverage, desired, registrations)
	leader.sessionsMutex.Unlock()
	coverage.pendingRepair = leader.repositoryPendingRepair(desired)
	return coverage
}

func activeRepositorySessionCount(
	sessions map[repositoryScaleSetSessionKey]*repositoryScaleSetSession,
	repository string,
) int {
	count := 0
	for key := range sessions {
		if key.repository == repository {
			count++
		}
	}
	return count
}

func (leader *clusterLeader) addDesiredSessionCoverage(
	coverage *repositorySessionCoverage,
	desired desiredRepositoryGeneration,
	registrations map[repositoryScaleSetSessionKey]repositorystate.Registration,
) {
	for _, status := range desired.statuses {
		key := repositoryScaleSetSessionKey{repository: desired.repository.URL, scaleSet: status.Name}
		session := leader.sessions[key]
		observed := session != nil && session.assignedJobs != nil &&
			session.assignedJobs.controllerEpoch == leader.repositoryControllerEpoch && session.assignedJobs.latest() != nil
		if observed {
			coverage.currentEpochObserved++
		}
		if repositorySessionReady(
			session, desired.repository.InstallationID, status, registrations[key],
			leader.repositoryControllerEpoch, leader.repositoryControlStarted,
		) {
			coverage.ready++
			continue
		}
		coverage.missingScaleSets = append(coverage.missingScaleSets, status.Name)
	}
}

func (leader *clusterLeader) repositoryPendingRepair(desired desiredRepositoryGeneration) int {
	leader.repairMutex.Lock()
	defer leader.repairMutex.Unlock()
	if repair, pending := leader.pendingRepositoryRepairs[desired.repository.URL]; pending && repair.generation == desired.generation {
		return 1
	}
	return 0
}

func (leader *clusterLeader) confirmedRegistrationSnapshot() map[repositoryScaleSetSessionKey]repositorystate.Registration {
	leader.registrationMutex.Lock()
	defer leader.registrationMutex.Unlock()
	registrations := make(map[repositoryScaleSetSessionKey]repositorystate.Registration, len(leader.confirmedRegistrations))
	for key, registration := range leader.confirmedRegistrations {
		registrations[key] = registration
	}
	return registrations
}

func repositorySessionReady(
	session *repositoryScaleSetSession,
	installationID githubapp.InstallationID,
	status cluster.ScaleSetStatus,
	registration repositorystate.Registration,
	controllerEpoch int64,
	requirePersistedRegistration bool,
) bool {
	registrationMatches := !requirePersistedRegistration ||
		(registration.GitHubScaleSetID == sessionRunnerScaleSetID(session) &&
			registration.RunnerGroup == status.RunnerGroup)
	return session != nil && session.runnerScaleSet != nil && session.capacity == status.EffectiveCapacity &&
		session.installationID == installationID && registrationMatches && session.assignedJobs != nil &&
		session.assignedJobs.controllerEpoch == controllerEpoch && session.assignedJobs.latest() != nil &&
		sessionPollerRunning(session.pollerDone)
}

func sessionRunnerScaleSetID(session *repositoryScaleSetSession) int {
	if session == nil || session.runnerScaleSet == nil {
		return 0
	}
	return session.runnerScaleSet.ID
}

func sessionPollerRunning(done <-chan struct{}) bool {
	if done == nil {
		return false
	}
	select {
	case <-done:
		return false
	default:
		return true
	}
}

func (leader *clusterLeader) logRepositorySessionCoverage(
	ctx context.Context,
	desired desiredRepositoryGeneration,
	coverage repositorySessionCoverage,
) {
	phase := "incomplete"
	if coverage.complete() {
		phase = "complete"
	}
	logAction(ctx, leader.daemon.logger, slog.LevelInfo, "repository_session_coverage", phase,
		slog.String("repository", desired.repository.URL),
		slog.Int("desired", coverage.desired),
		slog.Int("ready", coverage.ready),
		slog.Int("currentEpochObserved", coverage.currentEpochObserved),
		slog.Int("pendingRepair", coverage.pendingRepair),
		slog.Any("missingScaleSets", coverage.missingScaleSets),
		slog.Uint64("generation", desired.generation),
		slog.Int64("epoch", leader.repositoryControllerEpoch),
	)
}
