package daemon

import (
	"fmt"
	"io"
	"io/fs"
	"path/filepath"

	"github.com/stateshifters/runnerfoundry/internal/config"
)

const bytesPerMebibyte int64 = 1024 * 1024

type rotatingFileSettings struct {
	Path         string
	MaxSizeBytes int64
	MaxBackups   int
	FileMode     fs.FileMode
}

type productionLogDestinationFactory struct {
	standardOutput   io.Writer
	standardError    io.Writer
	ensureDirectory  func(string, fs.FileMode) error
	openRotatingFile func(rotatingFileSettings, func(error)) (io.WriteCloser, error)
}

func (factory productionLogDestinationFactory) Open(logConfig config.LogConfig) (io.WriteCloser, error) {
	if logConfig.File == nil {
		return noOpWriteCloser{writer: factory.standardOutput}, nil
	}

	parentDirectory := filepath.Dir(logConfig.File.Path)
	if err := factory.ensureDirectory(parentDirectory, 0o750); err != nil {
		return nil, fmt.Errorf("create log file directory %q: %w", parentDirectory, err)
	}

	settings := rotatingFileSettings{
		Path:         logConfig.File.Path,
		MaxSizeBytes: int64(*logConfig.File.MaxSizeMB) * bytesPerMebibyte,
		MaxBackups:   *logConfig.File.MaxBackups,
		FileMode:     0o600,
	}

	destination, err := factory.openRotatingFile(settings, func(maintenanceError error) {
		if _, writeError := fmt.Fprintf(factory.standardError, "log file maintenance failed: %v\n", maintenanceError); writeError != nil {
			return
		}
	})
	if err != nil {
		return nil, fmt.Errorf("open rotating log file %q: %w", settings.Path, err)
	}
	return destination, nil
}
