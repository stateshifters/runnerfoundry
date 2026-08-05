package state

import "testing"

func TestCalculateAvailableRunnerPoolCapacity(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name                 string
		live, queued, trying int
		want                 int
	}{
		{name: "shared demand", live: 30, queued: 7, trying: 5, want: 18},
		{name: "exactly exhausted", live: 12, queued: 7, trying: 5, want: 0},
		{name: "overcommitted", live: 2, queued: 7, trying: 5, want: 0},
		{name: "claim preserves availability", live: 30, queued: 11, trying: 1, want: 18},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			available := CalculateAvailableRunnerPoolCapacity(testCase.live, testCase.queued, testCase.trying)
			if available != testCase.want {
				t.Fatalf("available capacity = %d, want %d", available, testCase.want)
			}
		})
	}
}
