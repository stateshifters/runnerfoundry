package libvirtxml

import (
	"fmt"
	"strings"

	upstream "libvirt.org/go/libvirtxml"
)

const (
	metadataNamespace  = "urn:runnerfoundry"
	jitOEMStringPrefix = "runnerfoundry.jit="
)

// DomainTemplate is immutable source XML used to render one transient runner
// domain per request.
type DomainTemplate struct {
	xml     string
	marshal func(*upstream.Domain) (string, error)
}

// NewDomainTemplate retains domainXML for later parsing by Render.
func NewDomainTemplate(domainXML string) DomainTemplate {
	return domainTemplateWithMarshaler(domainXML, func(domain *upstream.Domain) (string, error) { return domain.Marshal() })
}

func domainTemplateWithMarshaler(
	domainXML string,
	marshal func(*upstream.Domain) (string, error),
) DomainTemplate {
	return DomainTemplate{xml: domainXML, marshal: marshal}
}

// RenderRequest supplies source-neutral ownership metadata and opaque bootstrap
// data for one domain.
type RenderRequest struct {
	RunnerPoolName string
	RunnerName     string
	JITConfig      string
}

var xmlTextEscaper = strings.NewReplacer(
	"&", "&amp;",
	"<", "&lt;",
	">", "&gt;",
	`"`, "&#34;",
	`'`, "&#39;",
)

// Render parses a fresh copy of the template, replaces RunnerFoundry-owned
// metadata and JIT OEM data, and leaves the original template unchanged.
func (template DomainTemplate) Render(request RenderRequest) (string, error) {
	var domain upstream.Domain
	if err := domain.Unmarshal(template.xml); err != nil {
		return "", fmt.Errorf("parse domain xml: %w", err)
	}
	domain.Name = request.RunnerName
	ensureSMBIOSSystemInfo(&domain)
	addJITOEMString(&domain, request.JITConfig)
	domain.Metadata = &upstream.DomainMetadata{
		XML: runnerMetadata(request),
	}
	renderedXML, err := template.marshal(&domain)
	if err != nil {
		return "", fmt.Errorf("marshal domain xml: %w", err)
	}
	return renderedXML, nil
}

func ensureSMBIOSSystemInfo(domain *upstream.Domain) {
	if domain.OS == nil {
		domain.OS = &upstream.DomainOS{}
	}
	domain.OS.SMBios = &upstream.DomainSMBios{Mode: "sysinfo"}
}

func addJITOEMString(domain *upstream.Domain, jitConfig string) {
	for sysInfoIndex := range domain.SysInfo {
		if domain.SysInfo[sysInfoIndex].SMBIOS == nil {
			continue
		}
		addJITOEMStringToSMBIOS(domain.SysInfo[sysInfoIndex].SMBIOS, jitConfig)
		return
	}
	domain.SysInfo = append(domain.SysInfo, upstream.DomainSysInfo{
		SMBIOS: &upstream.DomainSysInfoSMBIOS{
			OEMStrings: &upstream.DomainSysInfoOEMStrings{
				Entry: []string{jitOEMStringPrefix + jitConfig},
			},
		},
	})
}

func addJITOEMStringToSMBIOS(smbios *upstream.DomainSysInfoSMBIOS, jitConfig string) {
	if smbios.OEMStrings == nil {
		smbios.OEMStrings = &upstream.DomainSysInfoOEMStrings{}
	}
	oemStrings := make([]string, 0, len(smbios.OEMStrings.Entry)+1)
	for _, oemString := range smbios.OEMStrings.Entry {
		if strings.HasPrefix(oemString, jitOEMStringPrefix) {
			continue
		}
		oemStrings = append(oemStrings, oemString)
	}
	smbios.OEMStrings.Entry = append(oemStrings, jitOEMStringPrefix+jitConfig)
}

func runnerMetadata(request RenderRequest) string {
	return fmt.Sprintf(
		`<runnerfoundry:runner xmlns:runnerfoundry="%s"><runnerfoundry:runner-pool>%s</runnerfoundry:runner-pool><runnerfoundry:name>%s</runnerfoundry:name></runnerfoundry:runner>`,
		metadataNamespace,
		escapeXML(request.RunnerPoolName),
		escapeXML(request.RunnerName),
	)
}

func escapeXML(value string) string {
	return xmlTextEscaper.Replace(value)
}
