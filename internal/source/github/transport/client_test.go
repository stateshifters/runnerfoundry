package transport

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"reflect"
	"testing"
	"time"

	upstream "github.com/actions/scaleset"
)

func TestTransportClientOpensSiblingSessionWhileAnotherOpenIsBlocked(t *testing.T) {
	t.Parallel()
	api := &sessionConstructionAPIFake{started: make(chan struct{}), release: make(chan struct{})}
	client := clientFromUpstream("https://github.com/acme/widgets", api, api, nil)
	blocked := make(chan error, 1)
	go func() {
		_, err := client.OpenSession(t.Context(), Registration{ID: 29}, "member")
		blocked <- err
	}()
	<-api.started
	sibling := make(chan error, 1)
	go func() {
		_, err := client.OpenSession(t.Context(), Registration{ID: 30}, "member")
		sibling <- err
	}()
	select {
	case err := <-sibling:
		if err != nil {
			t.Errorf("open sibling session: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("blocked session prevented sibling from opening")
	}
	close(api.release)
	if err := <-blocked; err != nil {
		t.Fatalf("open blocked session: %v", err)
	}
}

type sessionConstructionAPIFake struct {
	upstreamAPIFake
	started chan struct{}
	release chan struct{}
}

func (api *sessionConstructionAPIFake) MessageSessionClient(
	_ context.Context,
	identifier int,
	_ string,
	_ ...upstream.HTTPOption,
) (*upstream.MessageSessionClient, error) {
	if identifier == 29 {
		close(api.started)
		<-api.release
	}
	return &upstream.MessageSessionClient{}, nil
}

func TestEnsureRunnerPoolCreatesMissingScaleSetWithRequiredSettings(t *testing.T) {
	t.Parallel()

	api := &actionsAPIFake{runnerGroup: &upstream.RunnerGroup{ID: 17}}
	client := Client{registrations: api}

	registration, err := client.EnsureRunnerPool(context.Background(), "linux", "default")
	if err != nil {
		t.Fatalf("ensure runner pool: %v", err)
	}
	if registration != (Registration{ID: 29, Name: "linux", RunnerGroupID: 17}) {
		t.Fatalf("registration = %#v", registration)
	}
	created := api.createdScaleSet
	if created == nil || created.Name != "linux" || created.RunnerGroupID != 17 {
		t.Fatalf("created scale set = %#v", created)
	}
	if len(created.Labels) != 1 || created.Labels[0] != (upstream.Label{Name: "linux", Type: "System"}) {
		t.Fatalf("created labels = %#v", created.Labels)
	}
	if !created.RunnerSetting.DisableUpdate {
		t.Fatal("runner update was not disabled")
	}
}

func TestEnsureRunnerPoolCreatesMissingScaleSetWithRunnerAutoUpdatesEnabled(t *testing.T) {
	t.Parallel()

	api := &actionsAPIFake{runnerGroup: &upstream.RunnerGroup{ID: 17}}
	client := Client{registrations: api, autoUpdateRunners: true}

	if _, err := client.EnsureRunnerPool(context.Background(), "linux", "default"); err != nil {
		t.Fatalf("ensure runner pool: %v", err)
	}
	if api.createdScaleSet == nil || api.createdScaleSet.RunnerSetting.DisableUpdate {
		t.Fatalf("created scale set = %#v", api.createdScaleSet)
	}
}

func TestResolveRunnerGroupReturnsConfiguredGroupID(t *testing.T) {
	t.Parallel()

	api := &actionsAPIFake{runnerGroup: &upstream.RunnerGroup{ID: 17}}
	client := Client{registrations: api}

	runnerGroupID, err := client.ResolveRunnerGroup(context.Background(), "default")
	if err != nil {
		t.Fatalf("ResolveRunnerGroup() error = %v", err)
	}
	if runnerGroupID != 17 {
		t.Fatalf("ResolveRunnerGroup() = %d, want 17", runnerGroupID)
	}
}

func TestResolveRunnerGroupRequiresRegistrationAPI(t *testing.T) {
	t.Parallel()

	for _, client := range []*Client{nil, {}} {
		if _, err := client.ResolveRunnerGroup(context.Background(), "default"); err == nil {
			t.Fatal("ResolveRunnerGroup() accepted a missing registration API")
		}
	}
}

func TestEnsureRunnerPoolReportsBoundaryFailures(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("API unavailable")
	tests := []struct {
		name   string
		client *Client
	}{
		{name: "missing API", client: &Client{}},
		{name: "runner group request", client: &Client{registrations: &actionsAPIFake{runnerGroupErr: expectedErr}}},
		{name: "missing runner group", client: &Client{registrations: &actionsAPIFake{}}},
		{name: "scale set request", client: &Client{registrations: &actionsAPIFake{
			runnerGroup: &upstream.RunnerGroup{ID: 17}, runnerScaleSetErr: expectedErr,
		}}},
		{name: "create request", client: &Client{registrations: &actionsAPIFake{
			runnerGroup: &upstream.RunnerGroup{ID: 17}, createErr: expectedErr,
		}}},
		{name: "empty create", client: &Client{registrations: &actionsAPIFake{
			runnerGroup: &upstream.RunnerGroup{ID: 17}, emptyCreate: true,
		}}},
	}
	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if _, err := testCase.client.EnsureRunnerPool(context.Background(), "linux", "default"); err == nil {
				t.Fatal("EnsureRunnerPool succeeded")
			}
		})
	}
}

func TestEnsureRunnerPoolReportsRunnerUpdateFailure(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("API unavailable")
	api := &actionsAPIFake{
		runnerGroup: &upstream.RunnerGroup{ID: 17},
		runnerScaleSet: &upstream.RunnerScaleSet{
			ID: 23, Name: "linux", RunnerGroupID: 17,
		},
		updateErr: expectedErr,
	}
	client := Client{registrations: api}

	if _, err := client.EnsureRunnerPool(context.Background(), "linux", "default"); !errors.Is(err, expectedErr) {
		t.Fatalf("update error = %v", err)
	}
}

func TestNewClientValidatesRepositoryAndUsesConfiguredRepository(t *testing.T) {
	if _, err := NewClient(Repository{URL: "://invalid"}, Credentials{}, nil, false); err == nil {
		t.Fatal("NewClient accepted invalid repository URL")
	}
	client, err := NewClient(
		Repository{InstallationID: 11, URL: "https://github.com/acme/widgets"},
		deterministicCredentials(t),
		nil,
		true,
	)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	if client.repositoryURL != "https://github.com/acme/widgets" {
		t.Fatalf("repository URL = %q", client.repositoryURL)
	}
	if !client.autoUpdateRunners {
		t.Fatal("runner auto-update setting was not retained")
	}
}

func TestClientFromUpstreamUsesNativeScaleSetRegistry(t *testing.T) {
	t.Parallel()

	api := &upstreamAPIFake{
		actionsAPIFake: actionsAPIFake{runnerGroup: &upstream.RunnerGroup{ID: 7}},
		registryScaleSets: []upstream.RunnerScaleSet{{
			ID: 11, Name: "obsolete", Labels: []upstream.Label{{Name: "obsolete", Type: "System"}},
			RunnerGroupID: 7, RunnerSetting: upstream.RunnerSetting{DisableUpdate: true},
		}},
	}
	client := clientFromUpstream("https://github.com/acme/widgets", api, api, nil)
	if _, err := client.PruneRunnerPools(context.Background(), 7, nil, []OwnedRunnerPool{{
		ID: 11, Name: "obsolete", RunnerGroupID: 7,
	}}); err != nil {
		t.Fatalf("prune through upstream registry: %v", err)
	}
	if len(api.deletedScaleSets) != 1 || api.deletedScaleSets[0] != 11 {
		t.Fatalf("deleted scale sets = %#v", api.deletedScaleSets)
	}
}

func deterministicCredentials(t *testing.T) Credentials {
	t.Helper()
	primeOne, validOne := new(big.Int).SetString("C49B48187124EE531F26C48AEBB4E3FD4C069D73DD3DA35D0AA87F46834A9BF3F2D3CD90BEDCB8199DBD2552CE670106A95ED0A3E0AE559FA68E846339D542CB", 16)
	primeTwo, validTwo := new(big.Int).SetString("F24E7544C243EC2DC2D3CD84F1B7CB1561FD7A25FE50D139AA338957EC2C5C64D68D8927518766C38CF2A0EA5B17F7D4C784B75B33F0316A0D82248F24533DF7", 16)
	if !validOne || !validTwo {
		t.Fatal("invalid deterministic RSA fixture")
	}
	one := big.NewInt(1)
	modulus := new(big.Int).Mul(primeOne, primeTwo)
	totient := new(big.Int).Mul(new(big.Int).Sub(primeOne, one), new(big.Int).Sub(primeTwo, one))
	privateKey := &rsa.PrivateKey{
		PublicKey: rsa.PublicKey{N: modulus, E: 65537},
		D:         new(big.Int).ModInverse(big.NewInt(65537), totient),
		Primes:    []*big.Int{primeOne, primeTwo},
	}
	encoded := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	return Credentials{AppID: 1, PrivateKey: string(encoded)}
}

func TestClientFromUpstreamBuildsAndReportsMessageSessions(t *testing.T) {
	t.Parallel()

	api := &upstreamAPIFake{session: &upstream.MessageSessionClient{}}
	client := clientFromUpstream("https://github.com/acme/widgets", api, api, nil)
	if _, err := client.OpenSession(context.Background(), Registration{ID: 17}, "owner"); err != nil {
		t.Fatalf("open adapted session: %v", err)
	}

	expectedErr := errors.New("session failed")
	api.sessionErr = expectedErr
	if _, err := client.OpenSession(context.Background(), Registration{ID: 17}, "owner"); !errors.Is(err, expectedErr) {
		t.Fatalf("open error = %v", err)
	}
}

func TestEnsureRunnerPoolReusesExistingScaleSet(t *testing.T) {
	t.Parallel()

	api := &actionsAPIFake{
		runnerGroup: &upstream.RunnerGroup{ID: 17},
		runnerScaleSet: &upstream.RunnerScaleSet{
			ID: 23, Name: "linux", RunnerGroupID: 17,
			RunnerSetting: upstream.RunnerSetting{DisableUpdate: true},
		},
	}
	client := Client{registrations: api}

	registration, err := client.EnsureRunnerPool(context.Background(), "linux", "default")
	if err != nil {
		t.Fatalf("ensure runner pool: %v", err)
	}
	if registration.ID != 23 {
		t.Fatalf("registration = %#v", registration)
	}
	if api.createdScaleSet != nil {
		t.Fatalf("unexpected create = %#v", api.createdScaleSet)
	}
	if api.updatedScaleSet != nil {
		t.Fatalf("unexpected update = %#v", api.updatedScaleSet)
	}
}

func TestEnsureRunnerPoolConvergesExistingRunnerUpdateSetting(t *testing.T) {
	t.Parallel()

	createdOn := time.Date(2026, time.October, 13, 9, 0, 0, 0, time.UTC)
	existing := &upstream.RunnerScaleSet{
		ID: 23, Name: "linux", RunnerGroupID: 17, RunnerGroupName: "default",
		Labels:        []upstream.Label{{Name: "linux", Type: "System"}},
		RunnerSetting: upstream.RunnerSetting{DisableUpdate: true}, CreatedOn: createdOn,
		RunnerJitConfigURL: "https://example.test/jit",
		Statistics:         &upstream.RunnerScaleSetStatistic{TotalAssignedJobs: 2},
	}
	api := &actionsAPIFake{runnerGroup: &upstream.RunnerGroup{ID: 17}, runnerScaleSet: existing}
	client := Client{registrations: api, autoUpdateRunners: true}

	registration, err := client.EnsureRunnerPool(context.Background(), "linux", "default")
	if err != nil {
		t.Fatalf("ensure runner pool: %v", err)
	}
	if registration != (Registration{ID: 23, Name: "linux", RunnerGroupID: 17}) {
		t.Fatalf("registration = %#v", registration)
	}
	expected := &upstream.RunnerScaleSet{RunnerSetting: upstream.RunnerSetting{DisableUpdate: false}}
	if api.updatedScaleSetID != 23 || !reflect.DeepEqual(api.updatedScaleSet, expected) {
		t.Fatalf("updated scale set = (%d, %#v), want (23, %#v)", api.updatedScaleSetID, api.updatedScaleSet, expected)
	}
}

type actionsAPIFake struct {
	runnerGroup       *upstream.RunnerGroup
	runnerGroupErr    error
	runnerScaleSet    *upstream.RunnerScaleSet
	runnerScaleSetErr error
	createdScaleSet   *upstream.RunnerScaleSet
	createErr         error
	emptyCreate       bool
	updatedScaleSetID int
	updatedScaleSet   *upstream.RunnerScaleSet
	updateErr         error
}

type upstreamAPIFake struct {
	actionsAPIFake
	jitAPIFake
	sessionErr        error
	session           *upstream.MessageSessionClient
	registryScaleSets []upstream.RunnerScaleSet
	deletedScaleSets  []int
}

func (api *upstreamAPIFake) ListRunnerScaleSets(context.Context, int) ([]upstream.RunnerScaleSet, error) {
	return api.registryScaleSets, nil
}

func (api *upstreamAPIFake) DeleteRunnerScaleSet(_ context.Context, runnerScaleSetID int) error {
	api.deletedScaleSets = append(api.deletedScaleSets, runnerScaleSetID)
	return nil
}

func (api *upstreamAPIFake) MessageSessionClient(
	context.Context,
	int,
	string,
	...upstream.HTTPOption,
) (*upstream.MessageSessionClient, error) {
	return api.session, api.sessionErr
}

func (api *actionsAPIFake) GetRunnerGroupByName(context.Context, string) (*upstream.RunnerGroup, error) {
	return api.runnerGroup, api.runnerGroupErr
}

func (api *actionsAPIFake) GetRunnerScaleSet(context.Context, int, string) (*upstream.RunnerScaleSet, error) {
	return api.runnerScaleSet, api.runnerScaleSetErr
}

func (api *actionsAPIFake) CreateRunnerScaleSet(
	_ context.Context,
	runnerScaleSet *upstream.RunnerScaleSet,
) (*upstream.RunnerScaleSet, error) {
	api.createdScaleSet = runnerScaleSet
	if api.createErr != nil || api.emptyCreate {
		return nil, api.createErr
	}
	return &upstream.RunnerScaleSet{
		ID: runnerScaleSet.RunnerGroupID + 12, Name: runnerScaleSet.Name, RunnerGroupID: runnerScaleSet.RunnerGroupID,
	}, nil
}

func (api *actionsAPIFake) UpdateRunnerScaleSet(
	_ context.Context,
	runnerScaleSetID int,
	runnerScaleSet *upstream.RunnerScaleSet,
) (*upstream.RunnerScaleSet, error) {
	api.updatedScaleSetID = runnerScaleSetID
	api.updatedScaleSet = runnerScaleSet
	return runnerScaleSet, api.updateErr
}
