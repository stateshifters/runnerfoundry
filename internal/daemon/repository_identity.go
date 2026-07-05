package daemon

import (
	"net/url"
	"strings"
	"unicode"
)

func localScaleSetName(repositoryURL string, scaleSetName string) string {
	return sanitizeName(repositoryIdentity(repositoryURL) + "-" + scaleSetName)
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
