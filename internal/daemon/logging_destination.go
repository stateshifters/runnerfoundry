package daemon

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/libtnb/logrotate"
	"github.com/meza/RunnerFoundry/internal/config"
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

func newProductionLogDestinationFactory() productionLogDestinationFactory {
	return productionLogDestinationFactory{
		standardOutput:  os.Stdout,
		standardError:   os.Stderr,
		ensureDirectory: os.MkdirAll,
		openRotatingFile: func(settings rotatingFileSettings, reportError func(error)) (io.WriteCloser, error) {
			return logrotate.New(
				settings.Path,
				logrotate.WithMaxSize(settings.MaxSizeBytes),
				logrotate.WithMaxBackups(settings.MaxBackups),
				logrotate.WithFileMode(settings.FileMode),
				logrotate.WithErrorHandler(reportError),
			)
		},
	}
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
