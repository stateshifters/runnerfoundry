package daemon

import (
	"io"
	"os"

	"github.com/libtnb/logrotate"
)

func newProductionLogDestinationFactory() productionLogDestinationFactory {
	return newProductionLogDestinationFactoryWithRotator(logrotate.New)
}

func newProductionLogDestinationFactoryWithRotator(
	openRotatingLogFile func(string, ...logrotate.Option) (*logrotate.Writer, error),
) productionLogDestinationFactory {
	return productionLogDestinationFactory{
		standardOutput:  os.Stdout,
		standardError:   os.Stderr,
		ensureDirectory: os.MkdirAll,
		openRotatingFile: func(settings rotatingFileSettings, reportError func(error)) (io.WriteCloser, error) {
			return openRotatingLogFile(
				settings.Path,
				logrotate.WithMaxSize(settings.MaxSizeBytes),
				logrotate.WithMaxBackups(settings.MaxBackups),
				logrotate.WithFileMode(settings.FileMode),
				logrotate.WithErrorHandler(reportError),
			)
		},
	}
}
