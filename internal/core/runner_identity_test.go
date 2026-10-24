package core

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestNewAttemptIDCreatesValidDistinctIdentities(t *testing.T) {
	first := NewAttemptID()
	second := NewAttemptID()
	if !first.IsValid() || !second.IsValid() || first == second || (AttemptID{}).IsValid() {
		t.Fatalf("attempt identities = %v and %v", first, second)
	}
}

func TestAttemptRunnerNameIncludesBothUniqueIdentities(t *testing.T) {
	jobID := uuid.MustParse("018fbb38-3d4e-7a58-a608-f23e1b764ffe")
	firstAttempt := AttemptID(uuid.MustParse("018fbb38-3d4e-7a58-a608-f23e1b764fff"))
	secondAttempt := AttemptID(uuid.MustParse("018fbb38-3d4e-7a58-a608-f23e1b764ffd"))
	first := AttemptRunnerName(jobID, firstAttempt)
	second := AttemptRunnerName(jobID, secondAttempt)
	if first == second || len(first) != 62 || len(second) != 62 {
		t.Fatalf("attempt names = %q and %q", first, second)
	}
}

func TestAttemptRunnerNameRoundTripsBothIdentities(t *testing.T) {
	jobID := uuid.MustParse("018fbb38-3d4e-7a58-a608-f23e1b764ffe")
	attemptID := AttemptID(uuid.MustParse("018fbb38-3d4e-7a58-a608-f23e1b764fff"))
	name := AttemptRunnerName(jobID, attemptID)

	identity, owned, err := ParseRunnerName(name)
	if err != nil {
		t.Fatalf("ParseRunnerName() error = %v", err)
	}
	if !owned || identity.JobID != jobID || identity.AttemptID != attemptID {
		t.Fatalf("ParseRunnerName() = (%#v, %t), want both attempt identities", identity, owned)
	}
}

func TestParseRunnerNameDistinguishesUnownedAndMalformedNames(t *testing.T) {
	identity, owned, err := ParseRunnerName("external-runner")
	if err != nil || owned || identity != (RunnerIdentity{}) {
		t.Fatalf("unowned ParseRunnerName() = (%#v, %t, %v)", identity, owned, err)
	}

	identity, owned, err = ParseRunnerName("runnerfoundry-v2-invalid")
	if !errors.Is(err, ErrInvalidRunnerName) || !owned || identity != (RunnerIdentity{}) {
		t.Fatalf("malformed ParseRunnerName() = (%#v, %t, %v)", identity, owned, err)
	}
}

func TestRunnerNameRecognitionRejectsMalformedIdentityTokens(t *testing.T) {
	validJob := "pzhRuH31TUy8jiOBkmdG2A"
	validAttempt := "AY-7OD1OelimCPI-G3ZP_w"
	for _, name := range []RunnerName{
		RunnerName("runnerfoundry-v2-!!!!!!!!!!!!!!!!!!!!!!-" + validAttempt),
		RunnerName("runnerfoundry-v2-" + validJob + "-!!!!!!!!!!!!!!!!!!!!!!"),
		"runnerfoundry-v2-AAAAAAAAAAAAAAAAAAAAAA-AAAAAAAAAAAAAAAAAAAAAA",
		RunnerName("runnerfoundry-v2-pzhRuH31TUy8jiOBkmdG2B-" + validAttempt),
		RunnerName("runnerfoundry-v2-" + validJob + "-AY-7OD1OelimCPI-G3ZP_x"),
	} {
		if IsRunnerFoundryRunnerName(name) {
			t.Fatalf("IsRunnerFoundryRunnerName(%q) = true", name)
		}
		if _, owned, err := ParseRunnerName(name); !owned || !errors.Is(err, ErrInvalidRunnerName) {
			t.Fatalf("ParseRunnerName(%q) = owned %t, error %v", name, owned, err)
		}
	}
}

func TestRunnerNameRecognitionAcceptsOnlyValidOwnedNames(t *testing.T) {
	valid := RunnerName("runnerfoundry-v2-pzhRuH31TUy8jiOBkmdG2A-AY-7OD1OelimCPI-G3ZP_w")
	if !IsRunnerFoundryRunnerName(valid) {
		t.Fatalf("IsRunnerFoundryRunnerName(%q) = false", valid)
	}
	for _, name := range []RunnerName{"external-runner", "runnerfoundry-v2-invalid"} {
		if IsRunnerFoundryRunnerName(name) {
			t.Fatalf("IsRunnerFoundryRunnerName(%q) = true", name)
		}
	}
}

func TestRunnerNameCreationRejectsZeroIdentities(t *testing.T) {
	validJob := uuid.MustParse("018fbb38-3d4e-7a58-a608-f23e1b764ffe")
	validAttempt := AttemptID(uuid.MustParse("018fbb38-3d4e-7a58-a608-f23e1b764fff"))
	for name, runnerName := range map[string]RunnerName{
		"zero job":     AttemptRunnerName(uuid.Nil, validAttempt),
		"zero attempt": AttemptRunnerName(validJob, AttemptID{}),
	} {
		t.Run(name, func(t *testing.T) {
			if runnerName != "" {
				t.Fatalf("AttemptRunnerName() = %q, want empty rejection", runnerName)
			}
		})
	}
}
