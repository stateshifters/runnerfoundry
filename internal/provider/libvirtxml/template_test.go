package libvirtxml

import (
	"errors"
	"strings"
	"testing"

	upstream "libvirt.org/go/libvirtxml"
)

func TestDomainTemplateInjectsUniqueNameAndMetadata(t *testing.T) {
	template := DomainTemplate{
		XML: `<domain type="kvm"><name>template</name><devices></devices></domain>`,
	}
	renderedXML, err := template.Render(RenderRequest{
		ScaleSetName: "linux-vm",
		RunnerName:   "linux-vm-1",
		JITConfig:    "secret",
	})
	if err != nil {
		t.Fatalf("expected render success, got %v", err)
	}
	for _, expected := range []string{
		"<name>linux-vm-1</name>",
		"github-scaler.scale-set",
		"linux-vm",
		"github-scaler.runner",
		"linux-vm-1",
		"github-scaler.jit-env",
	} {
		if !strings.Contains(renderedXML, expected) {
			t.Fatalf("expected rendered XML to contain %q:\n%s", expected, renderedXML)
		}
	}
	if strings.Contains(renderedXML, "secret") {
		t.Fatalf("jit config must not be embedded directly in XML: %s", renderedXML)
	}
}

func TestDomainTemplateRejectsInvalidXML(t *testing.T) {
	template := DomainTemplate{XML: `<domain><name>broken</domain>`}
	if _, err := template.Render(RenderRequest{RunnerName: "runner"}); err == nil {
		t.Fatal("expected invalid XML error")
	}
}

func TestDomainTemplateReplacesExistingMetadata(t *testing.T) {
	template := DomainTemplate{
		XML: `<domain type="kvm"><name>template</name><metadata><old>value</old></metadata></domain>`,
	}
	renderedXML, err := template.Render(RenderRequest{ScaleSetName: "set", RunnerName: "runner"})
	if err != nil {
		t.Fatalf("expected render success, got %v", err)
	}
	if !strings.Contains(renderedXML, "github-scaler.scale-set") {
		t.Fatalf("expected scaler metadata: %s", renderedXML)
	}
}

func TestDomainTemplateSurfacesMarshalFailures(t *testing.T) {
	originalMarshal := marshalDomainXML
	marshalDomainXML = func(domain *upstream.Domain) (string, error) {
		return "", errors.New("marshal failed")
	}
	defer func() {
		marshalDomainXML = originalMarshal
	}()
	template := DomainTemplate{XML: `<domain type="kvm"><name>template</name></domain>`}
	if _, err := template.Render(RenderRequest{RunnerName: "runner"}); err == nil {
		t.Fatal("expected marshal failure")
	}
}
