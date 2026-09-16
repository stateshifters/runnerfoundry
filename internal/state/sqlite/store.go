package sqlite

import statecontract "github.com/stateshifters/runnerfoundry/internal/state"

type Store struct {
	executor executor
	close    func() error
}

var _ statecontract.JobQueueStore = (*Store)(nil)
var _ statecontract.ProcessingJobStore = (*Store)(nil)
var _ statecontract.ProcessingTransitions = (*Store)(nil)
var _ statecontract.ProvisioningFailureTransitions = (*Store)(nil)
