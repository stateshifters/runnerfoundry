package libvirt

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/meza/RunnerFoundry/internal/provider/contract"
	"github.com/meza/RunnerFoundry/internal/provider/libvirtxml"
)

// RunnerState is libvirt's normalized lifecycle observation.
type RunnerState string

const (
	RunnerStateRunning   RunnerState = contract.RunnerStateRunning
	RunnerStateCompleted RunnerState = contract.RunnerStateCompleted
	RunnerStateUnknown   RunnerState = contract.RunnerStateUnknown
)

// RunnerInstance is one owned libvirt domain returned by List.
type RunnerInstance struct {
	ID             string
	RunnerName     string
	RunnerPoolName string
	State          RunnerState
}

// StartRequest contains ownership metadata and opaque JIT configuration for one
// ephemeral domain.
type StartRequest struct {
	RunnerPoolName string
	RunnerName     string
	JITConfig      string
}

// Config selects one libvirt connection and domain XML template path.
type Config struct {
	ConnectionURI     string
	DomainXMLTemplate string
}

// CommandRunner combines the libvirt command capabilities required by Provider.
type CommandRunner interface {
	DomainCreator
	DomainReader
	DomainDestroyer
	ConnectionReader
}

// DomainCreator creates a transient domain from rendered XML.
type DomainCreator interface {
	CreateDomain(ctx context.Context, connectionURI string, domainXML string) error
}

// DomainReader lists domains and obtains ownership and lifecycle evidence.
type DomainReader interface {
	ListDomains(ctx context.Context, connectionURI string) (string, error)
	ReadDomainXML(ctx context.Context, connectionURI string, domainName string) (string, error)
	ReadDomainState(ctx context.Context, connectionURI string, domainName string) (string, error)
}

// DomainDestroyer force-stops a transient domain by name.
type DomainDestroyer interface {
	DestroyDomain(ctx context.Context, connectionURI string, domainName string) error
}

// ConnectionReader verifies the configured libvirt connection.
type ConnectionReader interface {
	ReadConnectionURI(ctx context.Context, connectionURI string) (string, error)
}

// ExecCommandRunner invokes the local virsh CLI without retaining command state
// between calls.
type ExecCommandRunner struct {
	newCommand     func(context.Context, string, ...string) *exec.Cmd
	combinedOutput func(*exec.Cmd) ([]byte, error)
}

// NewExecCommandRunner constructs the production virsh command boundary.
func NewExecCommandRunner() ExecCommandRunner {
	return ExecCommandRunner{newCommand: exec.CommandContext, combinedOutput: (*exec.Cmd).CombinedOutput}
}

func (runner ExecCommandRunner) CreateDomain(ctx context.Context, connectionURI string, domainXML string) error {
	_, err := runner.runVirsh(ctx, connectionURI, domainXML, "create", "/dev/stdin")
	return err
}

func (runner ExecCommandRunner) ListDomains(ctx context.Context, connectionURI string) (string, error) {
	return runner.runVirsh(ctx, connectionURI, "", "list", "--all", "--name")
}

func (runner ExecCommandRunner) ReadDomainXML(ctx context.Context, connectionURI string, domainName string) (string, error) {
	return runner.runVirsh(ctx, connectionURI, "", "dumpxml", domainName)
}

func (runner ExecCommandRunner) ReadDomainState(ctx context.Context, connectionURI string, domainName string) (string, error) {
	return runner.runVirsh(ctx, connectionURI, "", "domstate", domainName)
}

func (runner ExecCommandRunner) DestroyDomain(ctx context.Context, connectionURI string, domainName string) error {
	_, err := runner.runVirsh(ctx, connectionURI, "", "destroy", domainName)
	return err
}

func (runner ExecCommandRunner) ReadConnectionURI(ctx context.Context, connectionURI string) (string, error) {
	return runner.runVirsh(ctx, connectionURI, "", "uri")
}

func (runner ExecCommandRunner) runVirsh(
	ctx context.Context,
	connectionURI string,
	stdin string,
	operation string,
	operands ...string,
) (string, error) {
	arguments := append([]string{"-c", connectionURI, operation}, operands...)
	execCommand := runner.newCommand(ctx, "virsh", arguments...)
	if stdin != "" {
		execCommand.Stdin = strings.NewReader(stdin)
	}
	outputBytes, err := runner.combinedOutput(execCommand)
	if err != nil {
		return "", fmt.Errorf("virsh %s failed: %w", operation, err)
	}
	return string(outputBytes), nil
}

// Provider owns transient domains bearing RunnerFoundry metadata for Config.
// Callers must Validate before Start.
type Provider struct {
	CommandRunner CommandRunner
	Config        Config
	readTemplate  func(string) ([]byte, error)
}

// NewProvider constructs a provider that reads Config.DomainXMLTemplate from
// the operating-system filesystem when validating or starting a runner.
func NewProvider(commandRunner CommandRunner, config Config) Provider {
	return NewProviderWithTemplateReader(commandRunner, config, os.ReadFile)
}

// NewProviderWithTemplateReader constructs a provider with an explicit template
// input boundary. It performs no I/O until Validate or Start.
func NewProviderWithTemplateReader(
	commandRunner CommandRunner,
	config Config,
	readTemplate func(string) ([]byte, error),
) Provider {
	return Provider{CommandRunner: commandRunner, Config: config, readTemplate: readTemplate}
}

// Start renders the configured template and creates one transient domain.
func (provider Provider) Start(ctx context.Context, request StartRequest) error {
	renderedXML, err := provider.renderDomainXML(request)
	if err != nil {
		return err
	}
	if err := provider.CommandRunner.CreateDomain(ctx, provider.Config.ConnectionURI, renderedXML); err != nil {
		return fmt.Errorf("create libvirt runner %q: %w", request.RunnerName, err)
	}
	return nil
}

// List returns all owned domains when requestedPools is empty, or filters by the
// first requested Runner Pool. It reads XML and state for each owned domain and
// fails without returning a partial result.
func (provider Provider) List(ctx context.Context, requestedPools ...string) ([]RunnerInstance, error) {
	output, err := provider.CommandRunner.ListDomains(ctx, provider.Config.ConnectionURI)
	if err != nil {
		return nil, fmt.Errorf("list libvirt domains: %w", err)
	}
	var instances []RunnerInstance
	for _, domainName := range strings.Split(output, "\n") {
		domainName = strings.TrimSpace(domainName)
		if domainName == "" {
			continue
		}
		instance, owned, err := provider.instanceForDomain(ctx, domainName, requestedPools...)
		if err != nil {
			return nil, err
		}
		if !owned {
			continue
		}
		instances = append(instances, listedInstance(instance, requestedPools))
	}
	return instances, nil
}

func listedInstance(instance RunnerInstance, requestedPools []string) RunnerInstance {
	if len(requestedPools) != 0 {
		instance.RunnerPoolName = ""
	}
	return instance
}

func (provider Provider) instanceForDomain(
	ctx context.Context,
	domainName string,
	requestedPools ...string,
) (RunnerInstance, bool, error) {
	domainXML, err := provider.CommandRunner.ReadDomainXML(ctx, provider.Config.ConnectionURI, domainName)
	if err != nil {
		return RunnerInstance{}, false, fmt.Errorf("dump libvirt domain %q: %w", domainName, err)
	}
	runnerPoolName := runnerPoolMetadataValue(domainXML)
	if runnerPoolName == "" {
		return RunnerInstance{}, false, nil
	}
	if len(requestedPools) != 0 && runnerPoolName != requestedPools[0] {
		return RunnerInstance{}, false, nil
	}
	domainState, err := provider.CommandRunner.ReadDomainState(ctx, provider.Config.ConnectionURI, domainName)
	if err != nil {
		return RunnerInstance{}, false, fmt.Errorf("read libvirt domain state %q: %w", domainName, err)
	}
	runnerName := metadataValue(domainXML, contract.OwnershipRunner)
	if runnerName == "" {
		runnerName = domainName
	}
	return RunnerInstance{
		ID:             domainName,
		RunnerName:     runnerName,
		RunnerPoolName: runnerPoolName,
		State:          libvirtState(domainState),
	}, true, nil
}

// Destroy force-stops a non-completed domain. Completed instances are already
// absent from the active lifecycle and are accepted as an idempotent no-op.
func (provider Provider) Destroy(ctx context.Context, instance RunnerInstance) error {
	if instance.State == RunnerStateCompleted {
		return nil
	}
	if err := provider.CommandRunner.DestroyDomain(ctx, provider.Config.ConnectionURI, instance.ID); err != nil {
		return fmt.Errorf("destroy libvirt runner %q: %w", instance.RunnerName, err)
	}
	return nil
}

// Validate reads and renders the template, then verifies the libvirt connection.
func (provider Provider) Validate(ctx context.Context) error {
	if _, err := provider.renderDomainXML(StartRequest{RunnerPoolName: "validation", RunnerName: "validation"}); err != nil {
		return err
	}
	if _, err := provider.CommandRunner.ReadConnectionURI(ctx, provider.Config.ConnectionURI); err != nil {
		return fmt.Errorf("validate libvirt connection: %w", err)
	}
	return nil
}

func (provider Provider) renderDomainXML(request StartRequest) (string, error) {
	if provider.readTemplate == nil {
		return "", errors.New("libvirt XML template reader is required")
	}
	templateBytes, err := provider.readTemplate(provider.Config.DomainXMLTemplate)
	if err != nil {
		return "", fmt.Errorf("read libvirt XML template: %w", err)
	}
	renderedXML, err := libvirtxml.NewDomainTemplate(string(templateBytes)).Render(libvirtxml.RenderRequest{
		RunnerPoolName: request.RunnerPoolName,
		RunnerName:     request.RunnerName,
		JITConfig:      request.JITConfig,
	})
	if err != nil {
		return "", fmt.Errorf("render libvirt XML template: %w", err)
	}
	return renderedXML, nil
}

func metadataValue(domainXML string, key string) string {
	namespacedValue := namespacedMetadataValue(domainXML, key)
	if namespacedValue != "" {
		return namespacedValue
	}
	return legacyMetadataValue(domainXML, key)
}

func runnerPoolMetadataValue(domainXML string) string {
	if value := metadataValue(domainXML, contract.OwnershipRunnerPool); value != "" {
		return value
	}
	return metadataValue(domainXML, contract.LegacyOwnershipScaleSet)
}

func namespacedMetadataValue(domainXML string, key string) string {
	var metadata domainMetadata
	if err := xml.Unmarshal([]byte(domainXML), &metadata); err != nil {
		return ""
	}
	switch key {
	case contract.OwnershipRunnerPool:
		return metadata.Metadata.Runner.RunnerPool
	case contract.LegacyOwnershipScaleSet:
		return metadata.Metadata.Runner.LegacyScaleSet
	case contract.OwnershipRunner:
		return metadata.Metadata.Runner.Name
	default:
		return ""
	}
}

func legacyMetadataValue(domainXML string, key string) string {
	openTag := "<" + key + ">"
	closeTag := "</" + key + ">"
	start := strings.Index(domainXML, openTag)
	if start < 0 {
		return ""
	}
	start += len(openTag)
	end := strings.Index(domainXML[start:], closeTag)
	if end < 0 {
		return ""
	}
	return domainXML[start : start+end]
}

type domainMetadata struct {
	Metadata scalerMetadata `xml:"metadata"`
}

type scalerMetadata struct {
	Runner scalerRunnerMetadata `xml:"urn:runnerfoundry runner"`
}

type scalerRunnerMetadata struct {
	RunnerPool     string `xml:"urn:runnerfoundry runner-pool"`
	LegacyScaleSet string `xml:"urn:runnerfoundry scale-set"`
	Name           string `xml:"urn:runnerfoundry name"`
}

func libvirtState(rawState string) RunnerState {
	switch strings.ToLower(strings.TrimSpace(rawState)) {
	case "running":
		return RunnerStateRunning
	case "shut off":
		return RunnerStateCompleted
	default:
		return RunnerStateUnknown
	}
}
