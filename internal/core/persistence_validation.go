package core

import "errors"

var ErrInvalidJobRequest = errors.New("invalid job request")
var ErrInvalidProcessingClaim = errors.New("invalid processing claim")

func (request JobRequest) Validate() error {
	if !request.Source.IsValid() || request.RunnerPool == "" || request.EnqueuedAt.IsZero() {
		return ErrInvalidJobRequest
	}
	if request.Scope != (DemandScope{}) && (!request.Scope.IsValid() || request.Scope.Adapter != request.Source.Adapter) {
		return ErrInvalidJobRequest
	}
	return nil
}

func (claim ProcessingClaim) Validate() error {
	if claim.MemberID == "" || !claim.AttemptID.IsValid() || claim.ClaimedAt.IsZero() || claim.InventoryEndpointID == "" {
		return ErrInvalidProcessingClaim
	}
	return nil
}
