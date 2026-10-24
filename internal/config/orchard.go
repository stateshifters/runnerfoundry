package config

import (
	"errors"
	"net/url"
	"path/filepath"
	"strings"
)

func (configured OrchardConfig) validate() error {
	controller, err := url.Parse(configured.ControllerURL)
	if err != nil || (controller.Scheme != "http" && controller.Scheme != "https") || controller.Hostname() == "" || controller.User != nil || controller.RawQuery != "" || controller.ForceQuery || controller.Fragment != "" || strings.Contains(configured.ControllerURL, "#") {
		return errors.New("orchard.controller_url must be an absolute HTTP(S) URL without credentials, query or fragment")
	}
	if strings.TrimSpace(configured.ServiceAccount) == "" {
		return errors.New("orchard.service_account is required")
	}
	if !filepath.IsAbs(configured.TokenFile) {
		return errors.New("orchard.token_file must be absolute")
	}
	if !filepath.IsAbs(configured.VMSpecFile) {
		return errors.New("orchard.vm_spec_file must be absolute")
	}
	if configured.TrustedCertificateFile != "" && !filepath.IsAbs(configured.TrustedCertificateFile) {
		return errors.New("orchard.trusted_certificate_file must be absolute")
	}
	return nil
}
