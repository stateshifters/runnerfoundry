package scaler

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

const (
	maximumRunnerNameLength = 64
	runnerNameHashLength    = 16
)

// runnerName builds the stable name shared by GitHub and the local runner provider.
// Names that already fit GitHub's constraints remain unchanged. Longer names retain
// their numeric identifier and use a deterministic hash to avoid truncation collisions.
func runnerName(baseName string, identifier int64) string {
	identifierText := strconv.FormatInt(identifier, 10)
	candidateName := fmt.Sprintf("%s-%s", baseName, identifierText)
	if len(candidateName) <= maximumRunnerNameLength && isSafeRunnerName(candidateName) {
		return candidateName
	}

	lowercaseBaseName := strings.ToLower(baseName)
	normalizedBaseName := normalizeRunnerNameBase(lowercaseBaseName)
	nameHash := sha256.Sum256([]byte(lowercaseBaseName))
	hashText := hex.EncodeToString(nameHash[:])[:runnerNameHashLength]
	prefixLength := maximumRunnerNameLength - len(hashText) - len(identifierText) - 2
	readablePrefix := normalizedBaseName
	if len(readablePrefix) > prefixLength {
		readablePrefix = readablePrefix[:prefixLength]
	}
	readablePrefix = strings.Trim(readablePrefix, "-")
	if readablePrefix == "" {
		readablePrefix = "runner"
	}
	return fmt.Sprintf("%s-%s-%s", readablePrefix, hashText, identifierText)
}

func isSafeRunnerName(name string) bool {
	for characterIndex := 0; characterIndex < len(name); characterIndex++ {
		character := name[characterIndex]
		if isSafeRunnerNameCharacter(character) {
			continue
		}
		return false
	}
	return true
}

func isSafeRunnerNameCharacter(character byte) bool {
	return character >= 'a' && character <= 'z' ||
		character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9' ||
		character == '-'
}

func normalizeRunnerNameBase(baseName string) string {
	normalizedName := make([]byte, 0, len(baseName))
	previousCharacterWasUnsafe := false
	for characterIndex := 0; characterIndex < len(baseName); characterIndex++ {
		character := baseName[characterIndex]
		if isSafeRunnerNameCharacter(character) {
			normalizedName = append(normalizedName, character)
			previousCharacterWasUnsafe = false
			continue
		}
		if !previousCharacterWasUnsafe {
			normalizedName = append(normalizedName, '-')
			previousCharacterWasUnsafe = true
		}
	}
	return strings.Trim(string(normalizedName), "-")
}
