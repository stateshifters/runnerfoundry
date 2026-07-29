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
	inspectPath      func(string) (fs.FileInfo, error)
	openRotatingFile func(rotatingFileSettings, func(error)) (io.WriteCloser, error)
}

func newProductionLogDestinationFactory() productionLogDestinationFactory {
	return productionLogDestinationFactory{
		standardOutput: os.Stdout,
		standardError:  os.Stderr,
		inspectPath:    os.Stat,
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
	info, err := factory.inspectPath(parentDirectory)
	if err != nil {
		return nil, fmt.Errorf("inspect log file directory %q: %w", parentDirectory, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("log file parent %q is not a directory", parentDirectory)
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
