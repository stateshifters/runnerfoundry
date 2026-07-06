package libvirtxml

import (
	"fmt"
	"strings"

	upstream "libvirt.org/go/libvirtxml"
)

const (
	MetadataNamespace  = "urn:github-scaler"
	JITOEMStringPrefix = "github-scaler.jit="
)

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

var xmlTextEscaper = strings.NewReplacer(
	"&", "&amp;",
	"<", "&lt;",
	">", "&gt;",
	`"`, "&#34;",
	`'`, "&#39;",
)

func (template DomainTemplate) Render(request RenderRequest) (string, error) {
	var domain upstream.Domain
	if err := domain.Unmarshal(template.XML); err != nil {
		return "", fmt.Errorf("parse domain xml: %w", err)
	}
	domain.Name = request.RunnerName
	ensureSMBIOSSystemInfo(&domain)
	addJITOEMString(&domain, request.JITConfig)
	domain.Metadata = &upstream.DomainMetadata{
		XML: scalerMetadata(request),
	}
	renderedXML, err := marshalDomainXML(&domain)
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
		`<github-scaler:runner xmlns:github-scaler="%s"><github-scaler:scale-set>%s</github-scaler:scale-set><github-scaler:name>%s</github-scaler:name></github-scaler:runner>`,
		MetadataNamespace,
		escapeXML(request.ScaleSetName),
		escapeXML(request.RunnerName),
	)
}

func escapeXML(value string) string {
	return xmlTextEscaper.Replace(value)
}
