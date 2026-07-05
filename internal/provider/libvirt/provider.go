package libvirt

import (
	"context"
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
	if _, err := provider.runVirshWithStdin(ctx, renderedXML, "define", "/dev/stdin"); err != nil {
		return fmt.Errorf("define libvirt runner %q: %w", request.RunnerName, err)
	}
	if _, err := provider.runVirsh(ctx, "start", request.RunnerName); err != nil {
		return fmt.Errorf("start libvirt runner %q: %w", request.RunnerName, err)
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
		domainXML, err := provider.runVirsh(ctx, "dumpxml", domainName)
		if err != nil {
			return nil, fmt.Errorf("dump libvirt domain %q: %w", domainName, err)
		}
		if metadataValue(domainXML, "github-scaler.scale-set") != scaleSetName {
			continue
		}
		domainState, err := provider.runVirsh(ctx, "domstate", domainName)
		if err != nil {
			return nil, fmt.Errorf("read libvirt domain state %q: %w", domainName, err)
		}
		runnerName := metadataValue(domainXML, "github-scaler.runner")
		if runnerName == "" {
			runnerName = domainName
		}
		instances = append(instances, RunnerInstance{
			ID:         domainName,
			RunnerName: runnerName,
			State:      libvirtState(domainState),
		})
	}
	return instances, nil
}

func (provider Provider) Destroy(ctx context.Context, instance RunnerInstance) error {
	if _, err := provider.runVirsh(ctx, "undefine", instance.ID); err != nil {
		return fmt.Errorf("undefine libvirt runner %q: %w", instance.RunnerName, err)
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
	return libvirtxml.DomainTemplate{XML: string(templateBytes)}.Render(libvirtxml.RenderRequest{
		ScaleSetName: request.ScaleSetName,
		RunnerName:   request.RunnerName,
		JITConfig:    request.JITConfig,
	})
}

func (provider Provider) runVirsh(ctx context.Context, args ...string) (string, error) {
	return provider.CommandRunner.Run(ctx, Command{
		Name: "virsh",
		Args: append([]string{"-c", provider.Config.ConnectionURI}, args...),
	})
}

func (provider Provider) runVirshWithStdin(ctx context.Context, stdin string, args ...string) (string, error) {
	return provider.CommandRunner.Run(ctx, Command{
		Name:  "virsh",
		Args:  append([]string{"-c", provider.Config.ConnectionURI}, args...),
		Stdin: stdin,
	})
}

func metadataValue(domainXML string, key string) string {
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

func libvirtState(rawState string) RunnerState {
	if strings.EqualFold(strings.TrimSpace(rawState), "running") {
		return RunnerStateRunning
	}
	return RunnerStateCompleted
}
