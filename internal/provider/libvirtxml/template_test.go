package libvirtxml

import (
	"encoding/xml"
	"errors"
	"slices"
	"strings"
	"testing"

	upstream "libvirt.org/go/libvirtxml"
)

type renderedDomain struct {
	Name     string `xml:"name"`
	Metadata struct {
		Runner struct {
			PoolName string `xml:"urn:runnerfoundry runner-pool"`
			Name     string `xml:"urn:runnerfoundry name"`
		} `xml:"urn:runnerfoundry runner"`
	} `xml:"metadata"`
	SysInfo struct {
		OEMStrings []string `xml:"oemStrings>entry"`
	} `xml:"sysinfo"`
}

func TestDomainTemplatePreservesIdentityText(t *testing.T) {
	template := NewDomainTemplate(`<domain type="kvm"><name>template</name></domain>`)
	request := RenderRequest{
		RunnerPoolName: "pool\r\n\t<&>\"'日本語",
		RunnerName:     "runner\r\n\t<&>\"'é",
	}
	renderedXML, err := template.Render(request)
	if err != nil {
		t.Fatalf("expected render success, got %v", err)
	}
	domain := decodeRenderedDomain(t, renderedXML)
	if domain.Metadata.Runner.PoolName != request.RunnerPoolName {
		t.Errorf("runner pool = %q, want %q", domain.Metadata.Runner.PoolName, request.RunnerPoolName)
	}
	if domain.Metadata.Runner.Name != request.RunnerName {
		t.Errorf("runner name = %q, want %q", domain.Metadata.Runner.Name, request.RunnerName)
	}
}

func TestDomainTemplateIsolatesRenderedRequests(t *testing.T) {
	template := NewDomainTemplate(`<domain type="kvm"><name>template</name><sysinfo type="smbios"><oemStrings><entry>existing=value</entry><entry>runnerfoundry.jit=old</entry></oemStrings></sysinfo></domain>`)
	requests := []RenderRequest{
		{RunnerPoolName: "first-pool", RunnerName: "first-runner", JITConfig: "first<&\""},
		{RunnerPoolName: "second-pool", RunnerName: "second-runner", JITConfig: "second\r\n日本語"},
	}
	rendered := make([]string, len(requests))
	for index, request := range requests {
		var err error
		rendered[index], err = template.Render(request)
		if err != nil {
			t.Fatalf("render %q: %v", request.RunnerName, err)
		}
	}
	for index, request := range requests {
		domain := decodeRenderedDomain(t, rendered[index])
		if domain.Name != request.RunnerName || domain.Metadata.Runner.Name != request.RunnerName || domain.Metadata.Runner.PoolName != request.RunnerPoolName {
			t.Errorf("render %q has domain name %q and ownership %+v", request.RunnerName, domain.Name, domain.Metadata.Runner)
		}
		expectedOEMStrings := []string{"existing=value", "runnerfoundry.jit=" + request.JITConfig}
		if !slices.Equal(domain.SysInfo.OEMStrings, expectedOEMStrings) {
			t.Errorf("render %q OEM strings = %q, want %q", request.RunnerName, domain.SysInfo.OEMStrings, expectedOEMStrings)
		}
	}
}

func decodeRenderedDomain(t *testing.T, renderedXML string) renderedDomain {
	t.Helper()
	var domain renderedDomain
	if err := xml.Unmarshal([]byte(renderedXML), &domain); err != nil {
		t.Fatalf("decode rendered domain: %v", err)
	}
	return domain
}

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
		t.Fatalf("expected runner metadata: %s", renderedXML)
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
		t.Fatalf("expected old runner JIT OEM string replaced:\n%s", renderedXML)
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
	_, err := template.Render(RenderRequest{RunnerName: "runner"})
	if err == nil {
		t.Fatal("expected marshal failure")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected render error to retain marshal failure, got %v", err)
	}
}
