package github

import (
	"context"
	"errors"
	"fmt"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/google/uuid"
)

func (adapter *Adapter) reconcileInitialAssignedDemand(ctx context.Context, observedAt time.Time) (bool, error) {
	if !adapter.InitialStatisticsPending && adapter.InitialStatistics == nil {
		return false, nil
	}
	if adapter.InitialSessionID == uuid.Nil {
		return false, errors.New("GitHub initial statistics require a message session ID")
	}
	if adapter.InitialStatistics == nil {
		err := errors.New("GitHub message session is missing initial statistics")
		adapter.reportAssignedDemand("failed", AssignedDemand{Repository: adapter.Repository,
			RunnerPool: adapter.RunnerPool, ObservationID: "session:" + adapter.InitialSessionID.String(),
			ObservedAt: observedAt}, AssignedDemandResult{}, nil, 0, observedAt, err, ActivityDebug)
		return false, err
	}
	changed, err := adapter.reconcileAssignedDemand(ctx, adapter.InitialStatistics,
		"session:"+adapter.InitialSessionID.String(), 0, observedAt)
	if err != nil {
		return false, err
	}
	adapter.InitialStatistics = nil
	adapter.InitialStatisticsPending = false
	return changed, nil
}

func (adapter *Adapter) reconcileAssignedDemand(
	ctx context.Context,
	statistics *upstream.RunnerScaleSetStatistic,
	observationID string,
	messageID int64,
	observedAt time.Time,
) (bool, error) {
	demand := AssignedDemand{Repository: adapter.Repository, RunnerPool: adapter.RunnerPool,
		ObservationID: observationID, DesiredJobs: statistics.TotalAssignedJobs, ObservedAt: observedAt}
	started := adapter.Observer.currentTime()
	adapter.reportAssignedDemand("started", demand, AssignedDemandResult{}, statistics,
		messageID, started, nil, ActivityDebug)
	result, err := adapter.executeAssignedDemandReconciliation(ctx, demand)
	if err != nil {
		adapter.reportAssignedDemand("failed", demand, result, statistics, messageID, started, err, ActivityDebug)
		return false, markManagerActivityReported(fmt.Errorf("reconcile GitHub assigned demand: %w", err))
	}
	adapter.reportAssignedDemand("completed", demand, result, statistics, messageID, started, nil, ActivityDebug)
	changed := result.JobsAdded != 0 || result.JobsRemoved != 0
	if changed {
		adapter.reportAssignedDemand("completed", demand, result, statistics, messageID, started, nil, ActivityInfo)
	}
	return changed, nil
}

func (adapter *Adapter) executeAssignedDemandReconciliation(
	ctx context.Context,
	demand AssignedDemand,
) (AssignedDemandResult, error) {
	if err := demand.Validate(); err != nil {
		return AssignedDemandResult{}, err
	}
	result, err := adapter.AssignedDemand.ReconcileAssignedDemand(ctx, demand)
	if err != nil {
		return result, fmt.Errorf("persist assigned demand: %w", err)
	}
	return result, nil
}

func (adapter *Adapter) reportAssignedDemand(
	outcome string,
	demand AssignedDemand,
	result AssignedDemandResult,
	statistics *upstream.RunnerScaleSetStatistic,
	messageID int64,
	started time.Time,
	err error,
	visibility ActivityVisibility,
) {
	activity := ManagerActivity{Visibility: visibility, Operation: "assigned_demand", Stage: "reconcile",
		Outcome: outcome, Repository: adapter.Repository, RunnerPool: adapter.RunnerPool,
		InstallationID: adapter.InstallationID, RunnerGroupID: adapter.RunnerGroupID, ScaleSetID: adapter.ScaleSetID,
		MessageID: messageID, DesiredJobs: demand.DesiredJobs, ActiveJobs: result.ActiveJobs,
		QueuedJobs: result.QueuedJobs, ProcessingJobs: result.ProcessingJobs,
		JobsAdded: result.JobsAdded, JobsRemoved: result.JobsRemoved, Err: err}
	if statistics != nil {
		activity.AvailableJobs = statistics.TotalAvailableJobs
		activity.AcquiredJobs = statistics.TotalAcquiredJobs
		activity.RunningJobs = statistics.TotalRunningJobs
		activity.RegisteredRunners = statistics.TotalRegisteredRunners
		activity.BusyRunners = statistics.TotalBusyRunners
		activity.IdleRunners = statistics.TotalIdleRunners
	}
	activity.StateBackend, activity.StateOperation = durableStateFailureContext(err)
	if outcome != "started" {
		activity.Duration = adapter.Observer.currentTime().Sub(started)
	}
	adapter.Observer.report(activity)
}
