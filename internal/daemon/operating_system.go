package daemon

import (
	"io"
	"os"

	"github.com/libtnb/logrotate"
	"github.com/meza/RunnerFoundry/internal/scaler"
)

var (
	newRotatingLogFile    = logrotate.New
	openSQLiteDemandStore = scaler.OpenSQLiteDemandStore
)

func newProductionLogDestinationFactory() productionLogDestinationFactory {
	return productionLogDestinationFactory{
		standardOutput:  os.Stdout,
		standardError:   os.Stderr,
		ensureDirectory: os.MkdirAll,
		openRotatingFile: func(settings rotatingFileSettings, reportError func(error)) (io.WriteCloser, error) {
			return newRotatingLogFile(
				settings.Path,
				logrotate.WithMaxSize(settings.MaxSizeBytes),
				logrotate.WithMaxBackups(settings.MaxBackups),
				logrotate.WithFileMode(settings.FileMode),
				logrotate.WithErrorHandler(reportError),
			)
		},
	}
}
