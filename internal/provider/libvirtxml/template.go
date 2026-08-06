package libvirtxml

import (
	"fmt"
	"strings"

	"github.com/meza/RunnerFoundry/internal/provider/contract"
	upstream "libvirt.org/go/libvirtxml"
)

const (
	MetadataNamespace  = "urn:runnerfoundry"
	JITOEMStringPrefix = contract.JITOEMStringPrefix
)

type DomainTemplate struct {
	xml     string
	marshal func(*upstream.Domain) (string, error)
}

func NewDomainTemplate(domainXML string) DomainTemplate {
	return domainTemplateWithMarshaler(domainXML, func(domain *upstream.Domain) (string, error) { return domain.Marshal() })
}

func domainTemplateWithMarshaler(
	domainXML string,
	marshal func(*upstream.Domain) (string, error),
) DomainTemplate {
	return DomainTemplate{xml: domainXML, marshal: marshal}
}

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

func (template DomainTemplate) Render(request RenderRequest) (string, error) {
	var domain upstream.Domain
	if err := domain.Unmarshal(template.xml); err != nil {
		return "", fmt.Errorf("parse domain xml: %w", err)
	}
	domain.Name = request.RunnerName
	ensureSMBIOSSystemInfo(&domain)
	addJITOEMString(&domain, request.JITConfig)
	domain.Metadata = &upstream.DomainMetadata{
		XML: scalerMetadata(request),
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
				Entry: []string{JITOEMStringPrefix + jitConfig},
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
		if strings.HasPrefix(oemString, JITOEMStringPrefix) {
			continue
		}
		oemStrings = append(oemStrings, oemString)
	}
	smbios.OEMStrings.Entry = append(oemStrings, JITOEMStringPrefix+jitConfig)
}

func scalerMetadata(request RenderRequest) string {
	return fmt.Sprintf(
		`<runnerfoundry:runner xmlns:runnerfoundry="%s"><runnerfoundry:runner-pool>%s</runnerfoundry:runner-pool><runnerfoundry:name>%s</runnerfoundry:name></runnerfoundry:runner>`,
		MetadataNamespace,
		escapeXML(request.RunnerPoolName),
		escapeXML(request.RunnerName),
	)
}

func escapeXML(value string) string {
	return xmlTextEscaper.Replace(value)
}
