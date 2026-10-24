package daemon

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/stateshifters/runnerfoundry/internal/config"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	orchardprovider "github.com/stateshifters/runnerfoundry/internal/provider/orchard"
)

func TestProductionCompositionBuildsOrchardLauncherAndInventory(t *testing.T) {
	provider := &orchardCompositionProvider{
		providerFactoryInventoryFake: providerFactoryInventoryFake{endpointID: "orchard:https://orchard.example", instances: []execution.RunnerInstance{{ID: "vm-1"}}},
	}
	configured := config.Config{RunnerPools: []config.RunnerPoolConfig{{Name: "macos", Orchard: validOrchardPoolConfig()}}}
	providers, inventory, err := buildProductionProvidersWithDependencies(context.Background(), configured, "member-a", productionDependencies{
		readFile: readOrchardCompositionFile,
		newOrchardProvider: func(configured orchardprovider.Config) (execution.ExecutionProvider, error) {
			expected := orchardprovider.Config{ControllerURL: "https://orchard.example", ServiceAccount: "runnerfoundry", Token: "token-value", VMSpec: []byte(`{"image":"macos-runner"}`), TrustedCertificate: []byte("certificate")}
			if !reflect.DeepEqual(configured, expected) {
				t.Fatal("Orchard configuration was not resolved")
			}
			return provider, nil
		},
	})
	if err != nil || providers["macos"] == nil || !provider.validated {
		t.Fatalf("Orchard composition = %+v, validated %t, %v", providers, provider.validated, err)
	}
	instances, err := inventory.Inspect(context.Background())
	if err != nil || len(instances) != 1 || instances[0].ID != "vm-1" {
		t.Fatalf("Orchard inventory = %+v, %v", instances, err)
	}
}

func TestOrchardCompositionRejectsMissingFactoryAndConstructionFailure(t *testing.T) {
	configured := config.RunnerPoolConfig{Name: "macos", Orchard: validOrchardPoolConfig()}
	if _, err := buildExecutionProvider(context.Background(), configured, executionDependencies{}); err == nil {
		t.Fatal("missing Orchard factory accepted")
	}
	if _, err := buildExecutionProvider(context.Background(), configured, executionDependencies{newOrchardProvider: func(orchardprovider.Config) (execution.ExecutionProvider, error) {
		t.Fatal("constructed without file reader")
		return nil, errors.New("unexpected Orchard construction")
	}}); err == nil {
		t.Fatal("missing Orchard file reader accepted")
	}
	want := errors.New("invalid Orchard VM specification")
	_, err := buildExecutionProvider(context.Background(), configured, executionDependencies{
		readFile:           func(string) ([]byte, error) { return []byte("content"), nil },
		newOrchardProvider: func(orchardprovider.Config) (execution.ExecutionProvider, error) { return nil, want },
	})
	if !errors.Is(err, want) {
		t.Fatalf("Orchard construction error = %v", err)
	}
}

func validOrchardPoolConfig() *config.OrchardConfig {
	return &config.OrchardConfig{ControllerURL: "https://orchard.example", ServiceAccount: "runnerfoundry", TokenFile: "/token", VMSpecFile: "/vm.json", TrustedCertificateFile: "/cert.pem"}
}

func TestOrchardCompositionRejectsUnreadableGenerationFilesWithoutExposingContents(t *testing.T) {
	for _, failedPath := range []string{"/token", "/vm.json", "/cert.pem"} {
		t.Run(failedPath, func(t *testing.T) {
			_, err := buildExecutionProvider(context.Background(), config.RunnerPoolConfig{Name: "macos", Orchard: validOrchardPoolConfig()}, executionDependencies{
				readFile: unreadableOrchardFile(failedPath),
				newOrchardProvider: func(orchardprovider.Config) (execution.ExecutionProvider, error) {
					t.Fatal("constructed provider from unreadable files")
					return nil, errors.New("unexpected Orchard construction")
				},
			})
			if err == nil || !strings.HasPrefix(err.Error(), "read Orchard ") || strings.Contains(err.Error(), "credential-content") {
				t.Fatalf("read failure = %v", err)
			}
		})
	}
}

func TestOrchardCompositionReadsFilesForEachGeneration(t *testing.T) {
	token := "first-token"
	var resolved []orchardprovider.Config
	dependencies := executionDependencies{readFile: func(path string) ([]byte, error) { return []byte(token), nil }, newOrchardProvider: func(configured orchardprovider.Config) (execution.ExecutionProvider, error) {
		resolved = append(resolved, configured)
		return &orchardCompositionProvider{}, nil
	}}
	pool := config.RunnerPoolConfig{Name: "macos", Orchard: validOrchardPoolConfig()}
	if _, err := buildExecutionProvider(context.Background(), pool, dependencies); err != nil {
		t.Fatal(err)
	}
	token = "second-token"
	if _, err := buildExecutionProvider(context.Background(), pool, dependencies); err != nil {
		t.Fatal(err)
	}
	if resolved[0].Token != "first-token" || resolved[1].Token != "second-token" || string(resolved[0].VMSpec) != "first-token" || string(resolved[1].VMSpec) != "second-token" {
		t.Fatal("generation files were not independently resolved")
	}
}

func TestOrchardCompositionAllowsDefaultControllerTrust(t *testing.T) {
	configured := validOrchardPoolConfig()
	configured.TrustedCertificateFile = ""
	_, err := buildExecutionProvider(context.Background(), config.RunnerPoolConfig{Name: "macos", Orchard: configured}, executionDependencies{
		readFile: func(path string) ([]byte, error) {
			if path != "/token" && path != "/vm.json" {
				t.Fatal("read unexpected certificate")
			}
			return []byte("content"), nil
		},
		newOrchardProvider: func(configured orchardprovider.Config) (execution.ExecutionProvider, error) {
			if configured.TrustedCertificate != nil {
				t.Fatal("unexpected custom trust")
			}
			return &orchardCompositionProvider{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

type orchardCompositionProvider struct {
	providerFactoryInventoryFake
	validated bool
}

func (provider *orchardCompositionProvider) Validate(ctx context.Context) error {
	if _, present := ctx.Deadline(); !present {
		return errors.New("Orchard validation requires an operation deadline")
	}
	provider.validated = true
	return nil
}

func (*orchardCompositionProvider) Start(context.Context, execution.StartRequest) error {
	return nil
}

func TestProductionOrchardFactoryValidatesConfigurationBeforeContactingController(t *testing.T) {
	factory := newProductionDependencies().newOrchardProvider
	configured := orchardprovider.Config{ControllerURL: "https://orchard.example", ServiceAccount: "runnerfoundry", Token: "token", VMSpec: []byte(`{"image":"base"}`)}
	provider, err := factory(configured)
	if err != nil || provider.EndpointID() != "orchard:https://orchard.example" {
		t.Fatalf("construct Orchard provider = %T, %v", provider, err)
	}
	configured.Token = ""
	if _, err := factory(configured); err == nil {
		t.Fatal("empty Orchard token accepted")
	}
	configured.Token = "token"
	configured.VMSpec = []byte(`{"image":"base","unknown":"secret-content"}`)
	if _, err := factory(configured); err == nil || strings.Contains(err.Error(), "secret-content") {
		t.Fatalf("invalid specification error = %v", err)
	}
}

func readOrchardCompositionFile(path string) ([]byte, error) {
	switch path {
	case "/token":
		return []byte("token-value\n"), nil
	case "/vm.json":
		return []byte(`{"image":"macos-runner"}`), nil
	case "/cert.pem":
		return []byte("certificate"), nil
	}
	return nil, errors.New("unexpected file path")
}

func unreadableOrchardFile(failedPath string) config.ReadFile {
	return func(path string) ([]byte, error) {
		if path == failedPath {
			return nil, errors.New("credential-content")
		}
		return []byte("content"), nil
	}
}
