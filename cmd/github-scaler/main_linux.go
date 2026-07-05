//go:build linux

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	upstream "github.com/actions/scaleset"
	"github.com/meza/github-scaler/internal/config"
	dockerprovider "github.com/meza/github-scaler/internal/provider/docker"
	libvirtprovider "github.com/meza/github-scaler/internal/provider/libvirt"
	"github.com/meza/github-scaler/internal/scaler"
	scalesetpoller "github.com/meza/github-scaler/internal/scaleset"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("github-scaler", flag.ContinueOnError)
	configPath := flags.String("config", "", "path to scaler YAML config")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *configPath == "" {
		return fmt.Errorf("--config is required")
	}
	configFile, err := os.Open(*configPath)
	if err != nil {
		return fmt.Errorf("open config: %w", err)
	}
	defer configFile.Close()
	loadedConfig, err := config.Load(configFile)
	if err != nil {
		return err
	}
	token, err := loadedConfig.Token()
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if loadedConfig.Log.Format == "json" {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	client, err := upstream.NewClientWithPersonalAccessToken(upstream.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL:     loadedConfig.GitHub.URL,
		PersonalAccessToken: token,
		SystemInfo: upstream.SystemInfo{
			System:  "github-scaler",
			Version: "v1",
		},
	})
	if err != nil {
		return fmt.Errorf("create scaleset client: %w", err)
	}
	for _, scaleSetConfig := range loadedConfig.ScaleSets {
		if err := runScaleSet(ctx, logger, client, scaleSetConfig); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func runScaleSet(ctx context.Context, logger *slog.Logger, client *upstream.Client, scaleSetConfig config.ScaleSetConfig) error {
	runnerGroup, err := client.GetRunnerGroupByName(ctx, scaleSetConfig.RunnerGroup)
	if err != nil {
		return fmt.Errorf("get runner group %q: %w", scaleSetConfig.RunnerGroup, err)
	}
	runnerScaleSet, err := client.GetRunnerScaleSet(ctx, runnerGroup.ID, scaleSetConfig.Name)
	if err != nil {
		return fmt.Errorf("get runner scale set %q: %w", scaleSetConfig.Name, err)
	}
	if runnerScaleSet == nil {
		runnerScaleSet, err = client.CreateRunnerScaleSet(ctx, &upstream.RunnerScaleSet{
			Name:          scaleSetConfig.Name,
			RunnerGroupID: runnerGroup.ID,
			Labels: []upstream.Label{{
				Name: scaleSetConfig.Name,
				Type: "System",
			}},
			RunnerSetting: upstream.RunnerSetting{
				DisableUpdate: true,
			},
		})
		if err != nil {
			return fmt.Errorf("create runner scale set %q: %w", scaleSetConfig.Name, err)
		}
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "github-scaler"
	}
	messageClient, err := client.MessageSessionClient(ctx, runnerScaleSet.ID, hostname)
	if err != nil {
		return fmt.Errorf("create message session for %q: %w", scaleSetConfig.Name, err)
	}
	reconciler, err := buildReconciler(ctx, scaleSetConfig, client, runnerScaleSet.ID)
	if err != nil {
		return err
	}
	poller := scalesetpoller.Poller{
		Client:      messageClient,
		Handler:     scaleSetHandler{Reconciler: reconciler},
		MaxCapacity: scaleSetConfig.MaxRunners,
	}
	go func() {
		for ctx.Err() == nil {
			if err := poller.PollOnce(ctx); err != nil {
				logger.Error("poll failed", slog.String("scaleSet", scaleSetConfig.Name), slog.String("error", err.Error()))
				time.Sleep(5 * time.Second)
			}
		}
	}()
	return nil
}

func buildReconciler(ctx context.Context, scaleSetConfig config.ScaleSetConfig, client *upstream.Client, scaleSetID int) (scaler.Reconciler, error) {
	if scaleSetConfig.Docker != nil {
		provider := dockerProviderAdapter{
			Provider: dockerprovider.ComposeProvider{
				CommandRunner: dockerprovider.ExecCommandRunner{},
				Config: dockerprovider.ComposeConfig{
					ComposeFile: scaleSetConfig.Docker.ComposeFile,
					Service:     scaleSetConfig.Docker.Service,
				},
			},
		}
		if err := provider.Provider.Validate(ctx); err != nil {
			return scaler.Reconciler{}, fmt.Errorf("validate docker provider for %q: %w", scaleSetConfig.Name, err)
		}
		return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name:       scaleSetConfig.Name,
			MaxRunners: scaleSetConfig.MaxRunners,
			Provider:   provider,
			JITConfig:  scalesetJITGenerator{Client: client, ScaleSetID: scaleSetID},
		}}, nil
	}
	if scaleSetConfig.Libvirt != nil {
		provider := libvirtProviderAdapter{
			Provider: libvirtprovider.Provider{
				CommandRunner: libvirtprovider.ExecCommandRunner{},
				Config: libvirtprovider.Config{
					ConnectionURI:     scaleSetConfig.Libvirt.ConnectionURI,
					DomainXMLTemplate: scaleSetConfig.Libvirt.DomainXMLTemplate,
				},
			},
		}
		if err := provider.Provider.Validate(ctx); err != nil {
			return scaler.Reconciler{}, fmt.Errorf("validate libvirt provider for %q: %w", scaleSetConfig.Name, err)
		}
		return scaler.Reconciler{ScaleSet: scaler.ScaleSetRuntime{
			Name:       scaleSetConfig.Name,
			MaxRunners: scaleSetConfig.MaxRunners,
			Provider:   provider,
			JITConfig:  scalesetJITGenerator{Client: client, ScaleSetID: scaleSetID},
		}}, nil
	}
	return scaler.Reconciler{}, fmt.Errorf("scale set %q has no provider", scaleSetConfig.Name)
}

type scaleSetHandler struct {
	Reconciler scaler.Reconciler
}

func (handler scaleSetHandler) HandleMessage(ctx context.Context, message *upstream.RunnerScaleSetMessage) error {
	_, err := handler.Reconciler.Reconcile(ctx, message.Statistics.TotalAssignedJobs)
	return err
}

type scalesetJITGenerator struct {
	Client     *upstream.Client
	ScaleSetID int
}

func (generator scalesetJITGenerator) Generate(ctx context.Context, runnerName string) (string, error) {
	jitConfig, err := generator.Client.GenerateJitRunnerConfig(ctx, &upstream.RunnerScaleSetJitRunnerSetting{
		Name: runnerName,
	}, generator.ScaleSetID)
	if err != nil {
		return "", err
	}
	return jitConfig.EncodedJITConfig, nil
}

type dockerProviderAdapter struct {
	Provider dockerprovider.ComposeProvider
}

func (adapter dockerProviderAdapter) List(ctx context.Context, scaleSetName string) ([]scaler.RunnerInstance, error) {
	instances, err := adapter.Provider.List(ctx, scaleSetName)
	if err != nil {
		return nil, err
	}
	converted := make([]scaler.RunnerInstance, 0, len(instances))
	for _, instance := range instances {
		state := scaler.RunnerStateCompleted
		if instance.State == dockerprovider.RunnerStateRunning {
			state = scaler.RunnerStateRunning
		}
		converted = append(converted, scaler.RunnerInstance{
			ID:         instance.ID,
			RunnerName: instance.RunnerName,
			State:      state,
		})
	}
	return converted, nil
}

func (adapter dockerProviderAdapter) Start(ctx context.Context, request scaler.RunnerStartRequest) error {
	return adapter.Provider.Start(ctx, dockerprovider.StartRequest{
		ScaleSetName: request.ScaleSetName,
		RunnerName:   request.RunnerName,
		JITConfig:    request.JITConfig,
	})
}

func (adapter dockerProviderAdapter) Destroy(ctx context.Context, instance scaler.RunnerInstance) error {
	return adapter.Provider.Destroy(ctx, dockerprovider.RunnerInstance{
		ID:         instance.ID,
		RunnerName: instance.RunnerName,
		State:      dockerprovider.RunnerStateCompleted,
	})
}

type libvirtProviderAdapter struct {
	Provider libvirtprovider.Provider
}

func (adapter libvirtProviderAdapter) List(ctx context.Context, scaleSetName string) ([]scaler.RunnerInstance, error) {
	instances, err := adapter.Provider.List(ctx, scaleSetName)
	if err != nil {
		return nil, err
	}
	converted := make([]scaler.RunnerInstance, 0, len(instances))
	for _, instance := range instances {
		state := scaler.RunnerStateCompleted
		if instance.State == libvirtprovider.RunnerStateRunning {
			state = scaler.RunnerStateRunning
		}
		converted = append(converted, scaler.RunnerInstance{
			ID:         instance.ID,
			RunnerName: instance.RunnerName,
			State:      state,
		})
	}
	return converted, nil
}

func (adapter libvirtProviderAdapter) Start(ctx context.Context, request scaler.RunnerStartRequest) error {
	return adapter.Provider.Start(ctx, libvirtprovider.StartRequest{
		ScaleSetName: request.ScaleSetName,
		RunnerName:   request.RunnerName,
		JITConfig:    request.JITConfig,
	})
}

func (adapter libvirtProviderAdapter) Destroy(ctx context.Context, instance scaler.RunnerInstance) error {
	return adapter.Provider.Destroy(ctx, libvirtprovider.RunnerInstance{
		ID:         instance.ID,
		RunnerName: instance.RunnerName,
		State:      libvirtprovider.RunnerStateCompleted,
	})
}
