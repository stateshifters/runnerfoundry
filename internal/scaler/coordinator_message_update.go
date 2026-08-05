package scaler

import (
	"context"
	"errors"
	"fmt"
)

// ApplyMessageUpdate commits every durable effect from one scale-set message
// before scheduling or acknowledgement can proceed.
func (coordinator *Coordinator) ApplyMessageUpdate(
	ctx context.Context,
	update MessageUpdate,
) (MessageUpdateResult, error) {
	store, supported := coordinator.store.(MessageUpdateStore)
	if !supported {
		return MessageUpdateResult{}, errors.New("demand store does not support atomic message updates")
	}
	if err := coordinator.prepareMessageUpdate(ctx, &update); err != nil {
		return MessageUpdateResult{}, err
	}
	result, err := store.ApplyMessageUpdate(ctx, update)
	if err != nil {
		return MessageUpdateResult{}, fmt.Errorf("persist scale-set message update: %w", err)
	}
	if _, durable := coordinator.store.(CapacityObligationStore); !durable {
		coordinator.ObserveAssigned(update.Repository, update.AssignedJobs)
	}
	result.ControllerEpoch = update.ControllerEpoch
	return result, nil
}

func (coordinator *Coordinator) prepareMessageUpdate(ctx context.Context, update *MessageUpdate) error {
	coordinator.mutex.Lock()
	update.AppID = coordinator.controllerAppID
	update.ControllerEpoch = coordinator.controllerEpoch
	update.ScaleSet = coordinator.scaleSet
	coordinator.mutex.Unlock()
	classification := ClassifyAssignedCapacity(update.assignedCapacityStatistics())
	if err := coordinator.requestAssignedRunnerCleanup(
		ctx, update.Repository, classification, update.ObservedAt,
	); err != nil {
		return fmt.Errorf("request message-update runner cleanup: %w", err)
	}
	activeCapacity, err := coordinator.repositoryActiveCapacity(ctx, update.Repository)
	if err != nil {
		return fmt.Errorf("inspect message-update capacity: %w", err)
	}
	update.ActiveCapacity = activeCapacity
	return nil
}
