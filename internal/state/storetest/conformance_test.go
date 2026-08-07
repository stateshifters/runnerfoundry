package storetest

import "testing"

func TestConformanceDispatcherRunsEveryScenario(t *testing.T) {
	_ = OldestJob()
	_ = ExpectedProcessing()
	_ = ExpectedRegistrations()
	_ = FixedTime()
	called := make(map[Scenario]bool, len(conformanceExpectations))
	RunConformance(t, recordingFactory{called: called})
	if len(called) != len(conformanceExpectations) {
		t.Fatalf("called %d scenarios, want %d", len(called), len(conformanceExpectations))
	}
	reporter := &recordingReporter{}
	assertObservation(reporter, Observation{}, Observation{Capacity: 1})
	if !reporter.called {
		t.Fatal("mismatch was not reported")
	}
}

type recordingFactory struct{ called map[Scenario]bool }

func (factory recordingFactory) Exercise(_ *testing.T, scenario Scenario) Observation {
	factory.called[scenario] = true
	return conformanceExpectations[scenario]
}

type recordingReporter struct{ called bool }

func (reporter *recordingReporter) Fatalf(string, ...any) { reporter.called = true }
