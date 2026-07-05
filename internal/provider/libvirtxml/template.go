package libvirtxml

import (
	"fmt"
	"strings"

	upstream "libvirt.org/go/libvirtxml"
)

const JITConfigEnvironment = "ACTIONS_RUNNER_INPUT_JITCONFIG"

type DomainTemplate struct {
	XML string
}

type RenderRequest struct {
	ScaleSetName string
	RunnerName   string
	JITConfig    string
}

var marshalDomainXML = func(domain *upstream.Domain) (string, error) {
	return domain.Marshal()
}

func (template DomainTemplate) Render(request RenderRequest) (string, error) {
	var domain upstream.Domain
	if err := domain.Unmarshal(template.XML); err != nil {
		return "", fmt.Errorf("parse domain xml: %w", err)
	}
	domain.Name = request.RunnerName
	renderedXML, err := marshalDomainXML(&domain)
	if err != nil {
		return "", fmt.Errorf("marshal domain xml: %w", err)
	}
	metadata := fmt.Sprintf(`<metadata><github-scaler.scale-set>%s</github-scaler.scale-set><github-scaler.runner>%s</github-scaler.runner><github-scaler.jit-env>%s</github-scaler.jit-env></metadata>`, request.ScaleSetName, request.RunnerName, JITConfigEnvironment)
	if strings.Contains(renderedXML, "<metadata>") {
		return strings.Replace(renderedXML, "<metadata>", metadata, 1), nil
	}
	return strings.Replace(renderedXML, "</domain>", metadata+"</domain>", 1), nil
}
