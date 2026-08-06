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

type RunnerState string

const (
	RunnerStateRunning   RunnerState = contract.RunnerStateRunning
	RunnerStateCompleted RunnerState = contract.RunnerStateCompleted
	RunnerStateUnknown   RunnerState = contract.RunnerStateUnknown
)

type RunnerInstance struct {
	ID         string
	RunnerName string
	State      RunnerState
}

type StartRequest struct {
	ScaleSetName string
	RunnerName   string
	JITConfig    string
}

type Config struct {
	ConnectionURI     string
	DomainXMLTemplate string
}

type CommandRunner interface {
	DomainCreator
	DomainReader
	DomainDestroyer
	ConnectionReader
}

type DomainCreator interface {
	CreateDomain(ctx context.Context, connectionURI string, domainXML string) error
}

type DomainReader interface {
	ListDomains(ctx context.Context, connectionURI string) (string, error)
	ReadDomainXML(ctx context.Context, connectionURI string, domainName string) (string, error)
	ReadDomainState(ctx context.Context, connectionURI string, domainName string) (string, error)
}

type DomainDestroyer interface {
	DestroyDomain(ctx context.Context, connectionURI string, domainName string) error
}

type ConnectionReader interface {
	ReadConnectionURI(ctx context.Context, connectionURI string) (string, error)
}

type ExecCommandRunner struct {
	newCommand     func(context.Context, string, ...string) *exec.Cmd
	combinedOutput func(*exec.Cmd) ([]byte, error)
}

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

type Provider struct {
	CommandRunner CommandRunner
	Config        Config
	readTemplate  func(string) ([]byte, error)
}

func NewProvider(commandRunner CommandRunner, config Config) Provider {
	return NewProviderWithTemplateReader(commandRunner, config, os.ReadFile)
}

// NewProviderWithTemplateReader constructs a provider with an explicit template input boundary.
func NewProviderWithTemplateReader(
	commandRunner CommandRunner,
	config Config,
	readTemplate func(string) ([]byte, error),
) Provider {
	return Provider{CommandRunner: commandRunner, Config: config, readTemplate: readTemplate}
}

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

func (provider Provider) List(ctx context.Context, scaleSetName string) ([]RunnerInstance, error) {
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
		instance, owned, err := provider.instanceForDomain(ctx, domainName, scaleSetName)
		if err != nil {
			return nil, err
		}
		if owned {
			instances = append(instances, instance)
		}
	}
	return instances, nil
}

func (provider Provider) instanceForDomain(ctx context.Context, domainName string, scaleSetName string) (RunnerInstance, bool, error) {
	domainXML, err := provider.CommandRunner.ReadDomainXML(ctx, provider.Config.ConnectionURI, domainName)
	if err != nil {
		return RunnerInstance{}, false, fmt.Errorf("dump libvirt domain %q: %w", domainName, err)
	}
	if metadataValue(domainXML, contract.OwnershipScaleSet) != scaleSetName {
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
		ID:         domainName,
		RunnerName: runnerName,
		State:      libvirtState(domainState),
	}, true, nil
}

func (provider Provider) Destroy(ctx context.Context, instance RunnerInstance) error {
	if instance.State == RunnerStateCompleted {
		return nil
	}
	if err := provider.CommandRunner.DestroyDomain(ctx, provider.Config.ConnectionURI, instance.ID); err != nil {
		return fmt.Errorf("destroy libvirt runner %q: %w", instance.RunnerName, err)
	}
	return nil
}

func (provider Provider) Validate(ctx context.Context) error {
	if _, err := provider.renderDomainXML(StartRequest{ScaleSetName: "validation", RunnerName: "validation"}); err != nil {
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
	renderedXML, err := libvirtxml.DomainTemplate{XML: string(templateBytes)}.Render(libvirtxml.RenderRequest{
		ScaleSetName: request.ScaleSetName,
		RunnerName:   request.RunnerName,
		JITConfig:    request.JITConfig,
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

func namespacedMetadataValue(domainXML string, key string) string {
	var metadata domainMetadata
	if err := xml.Unmarshal([]byte(domainXML), &metadata); err != nil {
		return ""
	}
	switch key {
	case contract.OwnershipScaleSet:
		return metadata.Metadata.Runner.ScaleSet
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
	ScaleSet string `xml:"urn:runnerfoundry scale-set"`
	Name     string `xml:"urn:runnerfoundry name"`
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
