package libvirt

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/url"
	"os/exec"
	"strings"

	"github.com/stateshifters/runnerfoundry/internal/core"
	"github.com/stateshifters/runnerfoundry/internal/execution"
	"github.com/stateshifters/runnerfoundry/internal/provider/libvirtxml"
)

const (
	legacyRunnerPoolTag = "runnerfoundry.runner-pool"
	legacyScaleSetTag   = "runnerfoundry.scale-set"
	legacyRunnerTag     = "runnerfoundry.runner"
)

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
	commandRunner  CommandRunner
	config         Config
	domainTemplate []byte
	memberID       core.MemberID
}

var _ execution.ExecutionProvider = Provider{}

func NewProvider(
	commandRunner CommandRunner,
	config Config,
	domainTemplate []byte,
	memberID core.MemberID,
) Provider {
	return Provider{
		commandRunner:  commandRunner,
		config:         config,
		domainTemplate: append([]byte(nil), domainTemplate...),
		memberID:       memberID,
	}
}

func (provider Provider) EndpointID() execution.InventoryEndpointID {
	return execution.InventoryEndpointID("libvirt:" + normalizedURI(provider.config.ConnectionURI))
}

func normalizedURI(raw string) string {
	trimmed := strings.TrimSpace(raw)
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return trimmed
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Host = strings.ToLower(parsed.Host)
	return parsed.String()
}

func (provider Provider) KnowsEndpoint(endpointID execution.InventoryEndpointID) bool {
	return provider.EndpointID() == endpointID
}

func (provider Provider) CanInspectEndpointForMember(
	endpointID execution.InventoryEndpointID,
	memberID core.MemberID,
) bool {
	if !provider.KnowsEndpoint(endpointID) {
		return false
	}
	parsed, err := url.Parse(strings.TrimSpace(provider.config.ConnectionURI))
	if err == nil && parsed.Host != "" {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(string(provider.memberID)), strings.TrimSpace(string(memberID)))
}

func (provider Provider) Refresh(ctx context.Context) error {
	_, err := provider.Inspect(ctx)
	return err
}

func (provider Provider) InspectEndpoint(
	ctx context.Context,
	endpointID execution.InventoryEndpointID,
) ([]execution.RunnerInstance, error) {
	if !provider.KnowsEndpoint(endpointID) {
		return nil, fmt.Errorf("libvirt endpoint %q is not configured", endpointID)
	}
	instances, err := provider.Inspect(ctx)
	for index := range instances {
		instances[index].EndpointID = endpointID
	}
	return instances, err
}

func (provider Provider) Validate(ctx context.Context) error {
	if _, err := provider.renderDomainXML(execution.StartRequest{
		RunnerPool: "validation",
		RunnerName: "validation",
	}); err != nil {
		return fmt.Errorf("validate libvirt provider: %w", err)
	}
	if _, err := provider.commandRunner.ReadConnectionURI(ctx, provider.config.ConnectionURI); err != nil {
		return fmt.Errorf("validate libvirt provider: validate libvirt connection: %w", err)
	}
	return nil
}

func (provider Provider) Start(ctx context.Context, request execution.StartRequest) error {
	renderedXML, err := provider.renderDomainXML(request)
	if err != nil {
		return fmt.Errorf("start libvirt runner: %w", err)
	}
	if err := provider.commandRunner.CreateDomain(ctx, provider.config.ConnectionURI, renderedXML); err != nil {
		return fmt.Errorf("start libvirt runner: create libvirt runner %q: %w", request.RunnerName, err)
	}
	return nil
}

func (provider Provider) renderDomainXML(request execution.StartRequest) (string, error) {
	if provider.domainTemplate == nil {
		return "", errors.New("libvirt XML template is required")
	}
	renderedXML, err := libvirtxml.NewDomainTemplate(string(provider.domainTemplate)).Render(libvirtxml.RenderRequest{
		RunnerPoolName: string(request.RunnerPool),
		RunnerName:     string(request.RunnerName),
		JITConfig:      string(request.Bootstrap.Content()),
	})
	if err != nil {
		return "", fmt.Errorf("render libvirt XML template: %w", err)
	}
	return renderedXML, nil
}

func (provider Provider) Inspect(ctx context.Context) ([]execution.RunnerInstance, error) {
	output, err := provider.commandRunner.ListDomains(ctx, provider.config.ConnectionURI)
	if err != nil {
		return nil, fmt.Errorf("inspect libvirt runners: list libvirt domains: %w", err)
	}
	var instances []execution.RunnerInstance
	for _, domainName := range strings.Split(output, "\n") {
		domainName = strings.TrimSpace(domainName)
		if domainName == "" {
			continue
		}
		instance, owned, err := provider.instanceForDomain(ctx, domainName)
		if err != nil {
			return nil, fmt.Errorf("inspect libvirt runners: %w", err)
		}
		if owned {
			instances = append(instances, instance)
		}
	}
	return instances, nil
}

func (provider Provider) instanceForDomain(
	ctx context.Context,
	domainName string,
) (execution.RunnerInstance, bool, error) {
	domainXML, err := provider.commandRunner.ReadDomainXML(ctx, provider.config.ConnectionURI, domainName)
	if err != nil {
		return execution.RunnerInstance{}, false, fmt.Errorf("dump libvirt domain %q: %w", domainName, err)
	}
	ownership := parseOwnershipMetadata(domainXML)
	if ownership.runnerPool == "" {
		return execution.RunnerInstance{}, false, nil
	}
	domainState, err := provider.commandRunner.ReadDomainState(ctx, provider.config.ConnectionURI, domainName)
	if err != nil {
		return execution.RunnerInstance{}, false, fmt.Errorf("read libvirt domain state %q: %w", domainName, err)
	}
	runnerName := ownership.runnerName
	if runnerName == "" {
		runnerName = domainName
	}
	return execution.RunnerInstance{
		ID:         domainName,
		RunnerName: core.RunnerName(runnerName),
		RunnerPool: core.RunnerPool(ownership.runnerPool),
		State:      libvirtState(domainState),
	}, true, nil
}

func (provider Provider) Destroy(ctx context.Context, instance execution.RunnerInstance) error {
	if instance.State == execution.RunnerStateCompleted {
		return nil
	}
	if err := provider.commandRunner.DestroyDomain(ctx, provider.config.ConnectionURI, instance.ID); err != nil {
		return fmt.Errorf("destroy libvirt runner: destroy libvirt runner %q: %w", instance.RunnerName, err)
	}
	return nil
}

type ownershipMetadata struct {
	runnerPool string
	runnerName string
}

func parseOwnershipMetadata(domainXML string) ownershipMetadata {
	var metadata domainMetadata
	ownership := ownershipMetadata{}
	namespacedLegacyRunnerPool := ""
	if err := xml.Unmarshal([]byte(domainXML), &metadata); err == nil {
		ownership.runnerPool = metadata.Metadata.Runner.RunnerPool
		namespacedLegacyRunnerPool = metadata.Metadata.Runner.LegacyScaleSet
		ownership.runnerName = metadata.Metadata.Runner.Name
	}
	if ownership.runnerPool == "" {
		ownership.runnerPool = legacyMetadataValue(domainXML, legacyRunnerPoolTag)
	}
	if ownership.runnerPool == "" {
		ownership.runnerPool = namespacedLegacyRunnerPool
	}
	if ownership.runnerPool == "" {
		ownership.runnerPool = legacyMetadataValue(domainXML, legacyScaleSetTag)
	}
	if ownership.runnerName == "" {
		ownership.runnerName = legacyMetadataValue(domainXML, legacyRunnerTag)
	}
	return ownership
}

func legacyMetadataValue(domainXML string, tag string) string {
	openTag := "<" + tag + ">"
	closeTag := "</" + tag + ">"
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
	Metadata runnerMetadata `xml:"metadata"`
}

type runnerMetadata struct {
	Runner runnerOwnershipMetadata `xml:"urn:runnerfoundry runner"`
}

type runnerOwnershipMetadata struct {
	RunnerPool     string `xml:"urn:runnerfoundry runner-pool"`
	LegacyScaleSet string `xml:"urn:runnerfoundry scale-set"`
	Name           string `xml:"urn:runnerfoundry name"`
}

func libvirtState(rawState string) execution.RunnerState {
	switch strings.ToLower(strings.TrimSpace(rawState)) {
	case "running":
		return execution.RunnerStateRunning
	case "shut off":
		return execution.RunnerStateCompleted
	default:
		return execution.RunnerStateUnknown
	}
}
