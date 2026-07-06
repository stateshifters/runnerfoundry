package libvirt

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/meza/github-scaler/internal/provider/libvirtxml"
)

type RunnerState string

const (
	RunnerStateRunning   RunnerState = "running"
	RunnerStateCompleted RunnerState = "completed"
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

type Command struct {
	Name  string
	Args  []string
	Stdin string
}

type CommandRunner interface {
	Run(ctx context.Context, command Command) (string, error)
}

type ExecCommandRunner struct{}

func (runner ExecCommandRunner) Run(ctx context.Context, command Command) (string, error) {
	execCommand := exec.CommandContext(ctx, command.Name, command.Args...)
	if command.Stdin != "" {
		execCommand.Stdin = strings.NewReader(command.Stdin)
	}
	outputBytes, err := execCommand.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %s failed: %w: %s", command.Name, strings.Join(command.Args, " "), err, string(outputBytes))
	}
	return string(outputBytes), nil
}

type Provider struct {
	CommandRunner CommandRunner
	Config        Config
}

func (provider Provider) Start(ctx context.Context, request StartRequest) error {
	renderedXML, err := provider.renderDomainXML(request)
	if err != nil {
		return err
	}
	if _, err := provider.runVirshWithStdin(ctx, renderedXML, "create", "/dev/stdin"); err != nil {
		return fmt.Errorf("create libvirt runner %q: %w", request.RunnerName, err)
	}
	return nil
}

func (provider Provider) List(ctx context.Context, scaleSetName string) ([]RunnerInstance, error) {
	output, err := provider.runVirsh(ctx, "list", "--all", "--name")
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
	domainXML, err := provider.runVirsh(ctx, "dumpxml", domainName)
	if err != nil {
		return RunnerInstance{}, false, fmt.Errorf("dump libvirt domain %q: %w", domainName, err)
	}
	if metadataValue(domainXML, "github-scaler.scale-set") != scaleSetName {
		return RunnerInstance{}, false, nil
	}
	domainState, err := provider.runVirsh(ctx, "domstate", domainName)
	if err != nil {
		return RunnerInstance{}, false, fmt.Errorf("read libvirt domain state %q: %w", domainName, err)
	}
	runnerName := metadataValue(domainXML, "github-scaler.runner")
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
	if instance.State != RunnerStateRunning {
		return nil
	}
	if _, err := provider.runVirsh(ctx, "destroy", instance.ID); err != nil {
		return fmt.Errorf("destroy libvirt runner %q: %w", instance.RunnerName, err)
	}
	return nil
}

func (provider Provider) Validate(ctx context.Context) error {
	if _, err := provider.renderDomainXML(StartRequest{ScaleSetName: "validation", RunnerName: "validation"}); err != nil {
		return err
	}
	if _, err := provider.runVirsh(ctx, "uri"); err != nil {
		return fmt.Errorf("validate libvirt connection: %w", err)
	}
	return nil
}

func (provider Provider) renderDomainXML(request StartRequest) (string, error) {
	templateBytes, err := os.ReadFile(provider.Config.DomainXMLTemplate)
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

func (provider Provider) runVirsh(ctx context.Context, args ...string) (string, error) {
	output, err := provider.CommandRunner.Run(ctx, Command{
		Name: "virsh",
		Args: append([]string{"-c", provider.Config.ConnectionURI}, args...),
	})
	if err != nil {
		return "", fmt.Errorf("run virsh %q: %w", strings.Join(args, " "), err)
	}
	return output, nil
}

func (provider Provider) runVirshWithStdin(ctx context.Context, stdin string, args ...string) (string, error) {
	output, err := provider.CommandRunner.Run(ctx, Command{
		Name:  "virsh",
		Args:  append([]string{"-c", provider.Config.ConnectionURI}, args...),
		Stdin: stdin,
	})
	if err != nil {
		return "", fmt.Errorf("run virsh %q: %w", strings.Join(args, " "), err)
	}
	return output, nil
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
	case "github-scaler.scale-set":
		return metadata.Metadata.Runner.ScaleSet
	case "github-scaler.runner":
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
	Runner scalerRunnerMetadata `xml:"urn:github-scaler runner"`
}

type scalerRunnerMetadata struct {
	ScaleSet string `xml:"urn:github-scaler scale-set"`
	Name     string `xml:"urn:github-scaler name"`
}

func libvirtState(rawState string) RunnerState {
	if strings.EqualFold(strings.TrimSpace(rawState), "running") {
		return RunnerStateRunning
	}
	return RunnerStateCompleted
}
