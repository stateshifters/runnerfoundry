package libvirtxml

import (
	"errors"
	"strings"
	"testing"

	upstream "libvirt.org/go/libvirtxml"
)

func TestDomainTemplateInjectsUniqueNameAndMetadata(t *testing.T) {
	template := NewDomainTemplate(`<domain type="kvm"><name>template</name><devices></devices></domain>`)
	renderedXML, err := template.Render(RenderRequest{
		RunnerPoolName: "linux-vm",
		RunnerName:     "linux-vm-1",
		JITConfig:      "secret",
	})
	if err != nil {
		t.Fatalf("expected render success, got %v", err)
	}
	for _, expected := range []string{
		"<name>linux-vm-1</name>",
		`<smbios mode="sysinfo"></smbios>`,
		`<sysinfo type="smbios">`,
		`<oemStrings>`,
		`<entry>runnerfoundry.jit=secret</entry>`,
		`xmlns:runnerfoundry="urn:runnerfoundry"`,
		"runnerfoundry:runner-pool",
		"linux-vm",
		"runnerfoundry:name",
		"linux-vm-1",
	} {
		if !strings.Contains(renderedXML, expected) {
			t.Fatalf("expected rendered XML to contain %q:\n%s", expected, renderedXML)
		}
	}
	if strings.Contains(renderedXML, "runnerfoundry.jit-env") || strings.Contains(renderedXML, "ACTIONS_RUNNER_INPUT_JITCONFIG") {
		t.Fatalf("jit config must not be stored in metadata: %s", renderedXML)
	}
}

func TestDomainTemplateRejectsInvalidXML(t *testing.T) {
	template := NewDomainTemplate(`<domain><name>broken</domain>`)
	if _, err := template.Render(RenderRequest{RunnerName: "runner"}); err == nil {
		t.Fatal("expected invalid XML error")
	}
}

func TestDomainTemplateReplacesExistingMetadata(t *testing.T) {
	template := NewDomainTemplate(`<domain type="kvm"><name>template</name><metadata><old>value</old></metadata></domain>`)
	renderedXML, err := template.Render(RenderRequest{RunnerPoolName: "set", RunnerName: "runner"})
	if err != nil {
		t.Fatalf("expected render success, got %v", err)
	}
	if !strings.Contains(renderedXML, "runnerfoundry:runner-pool") {
		t.Fatalf("expected scaler metadata: %s", renderedXML)
	}
	if strings.Contains(renderedXML, "<old>value</old>") {
		t.Fatalf("expected old metadata replaced: %s", renderedXML)
	}
}

func TestDomainTemplateMergesExistingSMBIOSOEMStrings(t *testing.T) {
	template := NewDomainTemplate(`<domain type="kvm"><name>template</name><os><type arch="x86_64" machine="q35">hvm</type><smbios mode="host"/></os><sysinfo type="smbios"><oemStrings><entry>existing=value</entry><entry>runnerfoundry.jit=old</entry></oemStrings></sysinfo></domain>`)
	renderedXML, err := template.Render(RenderRequest{RunnerPoolName: "set", RunnerName: "runner", JITConfig: `jit<&"`})
	if err != nil {
		t.Fatalf("expected render success, got %v", err)
	}
	for _, expected := range []string{
		`<smbios mode="sysinfo"></smbios>`,
		`<entry>existing=value</entry>`,
		`<entry>runnerfoundry.jit=jit&lt;&amp;&#34;</entry>`,
	} {
		if !strings.Contains(renderedXML, expected) {
			t.Fatalf("expected rendered XML to contain %q:\n%s", expected, renderedXML)
		}
	}
	if strings.Contains(renderedXML, "runnerfoundry.jit=old") {
		t.Fatalf("expected old scaler JIT OEM string replaced:\n%s", renderedXML)
	}
}

func TestDomainTemplateAddsOEMStringsToExistingSMBIOS(t *testing.T) {
	template := NewDomainTemplate(`<domain type="kvm"><name>template</name><os><type arch="x86_64" machine="q35">hvm</type></os><sysinfo type="smbios"><system><entry name="manufacturer">test</entry></system></sysinfo></domain>`)
	renderedXML, err := template.Render(RenderRequest{RunnerPoolName: "set", RunnerName: "runner", JITConfig: "secret"})
	if err != nil {
		t.Fatalf("expected render success, got %v", err)
	}
	for _, expected := range []string{
		`<entry name="manufacturer">test</entry>`,
		`<oemStrings>`,
		`<entry>runnerfoundry.jit=secret</entry>`,
	} {
		if !strings.Contains(renderedXML, expected) {
			t.Fatalf("expected rendered XML to contain %q:\n%s", expected, renderedXML)
		}
	}
}

func TestDomainTemplatePreservesNonSMBIOSSysInfo(t *testing.T) {
	template := NewDomainTemplate(`<domain type="kvm"><name>template</name><sysinfo type="fwcfg"><entry name="opt/vendor/value">existing</entry></sysinfo></domain>`)
	renderedXML, err := template.Render(RenderRequest{RunnerPoolName: "set", RunnerName: "runner", JITConfig: "secret"})
	if err != nil {
		t.Fatalf("expected render success, got %v", err)
	}
	for _, expected := range []string{
		`<sysinfo type="fwcfg">`,
		`<entry name="opt/vendor/value">existing</entry>`,
		`<sysinfo type="smbios">`,
		`<entry>runnerfoundry.jit=secret</entry>`,
	} {
		if !strings.Contains(renderedXML, expected) {
			t.Fatalf("expected rendered XML to contain %q:\n%s", expected, renderedXML)
		}
	}
}

func TestDomainTemplateSurfacesMarshalFailures(t *testing.T) {
	wantErr := errors.New("marshal failed")
	template := domainTemplateWithMarshaler(`<domain type="kvm"><name>template</name></domain>`, func(*upstream.Domain) (string, error) {
		return "", wantErr
	})
	if _, err := template.Render(RenderRequest{RunnerName: "runner"}); err == nil {
		t.Fatal("expected marshal failure")
	}
}
