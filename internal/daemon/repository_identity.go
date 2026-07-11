package daemon

import (
	"fmt"
	"net/url"
	"strings"
	"unicode"

	"github.com/meza/github-scaler/internal/config"
)

func localScaleSetName(repositoryURL string, scaleSetName string) string {
	return sanitizeName(repositoryIdentity(repositoryURL) + "-" + scaleSetName)
}

func validateLocalScaleSetNames(repositoryURLs []string, scaleSetConfigs []config.ScaleSetConfig) error {
	owners := map[string]string{}
	for _, repositoryURL := range repositoryURLs {
		for _, scaleSetConfig := range scaleSetConfigs {
			localName := localScaleSetName(repositoryURL, scaleSetConfig.Name)
			owner := repositoryURL + " " + scaleSetConfig.Name
			if previousOwner, exists := owners[localName]; exists && previousOwner != owner {
				return fmt.Errorf("local scale set name %q is shared by %s and %s", localName, previousOwner, owner)
			}
			owners[localName] = owner
		}
	}
	return nil
}

func repositoryIdentity(repositoryURL string) string {
	parsedURL, err := url.Parse(repositoryURL)
	if err != nil || parsedURL.Host == "" {
		return repositoryURL
	}
	return strings.Trim(parsedURL.Host+parsedURL.Path, "/")
}

func sanitizeName(name string) string {
	var characters []rune
	previousDash := false
	for _, character := range strings.ToLower(name) {
		if unicode.IsLetter(character) || unicode.IsDigit(character) {
			characters = append(characters, character)
			previousDash = false
			continue
		}
		if !previousDash {
			characters = append(characters, '-')
			previousDash = true
		}
	}
	return strings.Trim(string(characters), "-")
}
