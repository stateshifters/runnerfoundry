package daemon

import "fmt"

func githubAssignedDemandMessage(event operationalEvent) string {
	if event.outcome == "failed" {
		return fmt.Sprintf("Failed to reconcile assigned GitHub demand for %s in Runner Pool %s",
			event.repository, event.runnerPool)
	}
	if event.jobsAdded > 0 && event.jobsRemoved == 0 {
		return fmt.Sprintf("Recovered %d assigned GitHub jobs for %s in Runner Pool %s",
			event.jobsAdded, event.repository, event.runnerPool)
	}
	if event.jobsRemoved > 0 && event.jobsAdded == 0 {
		return fmt.Sprintf("Removed %d excess assigned GitHub jobs for %s in Runner Pool %s",
			event.jobsRemoved, event.repository, event.runnerPool)
	}
	return fmt.Sprintf("Reconciled assigned GitHub demand for %s in Runner Pool %s",
		event.repository, event.runnerPool)
}
