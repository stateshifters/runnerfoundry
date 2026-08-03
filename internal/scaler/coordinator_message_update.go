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
	coordinator.mutex.Lock()
	update.AppID = coordinator.controllerAppID
	update.ControllerEpoch = coordinator.controllerEpoch
	update.ScaleSet = coordinator.scaleSet
	coordinator.mutex.Unlock()
	activeCapacity, err := coordinator.repositoryActiveCapacity(ctx, update.Repository)
	if err != nil {
		return MessageUpdateResult{}, fmt.Errorf("inspect message-update capacity: %w", err)
	}
	update.ActiveCapacity = activeCapacity
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
