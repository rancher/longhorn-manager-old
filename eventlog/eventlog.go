package eventlog

import (
	"github.com/Sirupsen/logrus"
	logrusSyslog "github.com/Sirupsen/logrus/hooks/syslog"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-manager/types"
	"log/syslog"
	"sync"
)

var (
	lock       = &sync.RWMutex{}
	nilWrapper = &LogWrapper{logger: theNilLogger}

	currentLogger = nilWrapper
)

func Get() types.EventLogger {
	lock.RLock()
	defer lock.RUnlock()

	return currentLogger
}

func Set(logger types.EventLogger) {
	lock.Lock()
	defer lock.Unlock()

	if logger == nil {
		currentLogger = nilWrapper
	} else {
		currentLogger = &LogWrapper{logger: logger}
	}
}

func Info(args ...interface{}) {
	Get().Info(args...)
}

func Infof(format string, args ...interface{}) {
	Get().Infof(format, args...)
}

func Error(args ...interface{}) {
	Get().Error(args...)
}

func Errorf(format string, args ...interface{}) {
	Get().Errorf(format, args...)
}

type nilLogger struct{}

var theNilLogger *nilLogger // = nil

func (*nilLogger) Info(...interface{})           {}
func (*nilLogger) Infof(string, ...interface{})  {}
func (*nilLogger) Error(...interface{})          {}
func (*nilLogger) Errorf(string, ...interface{}) {}

type LogWrapper struct {
	logger types.EventLogger
}

func (lw *LogWrapper) Info(args ...interface{}) {
	logrus.Debug("event log: Info() called: ", args)
	lw.logger.Info(args...)
}

func (lw *LogWrapper) Infof(format string, args ...interface{}) {
	logrus.Debug("event log: Infof() called: ", format, args)
	lw.logger.Infof(format, args...)
}

func (lw *LogWrapper) Error(args ...interface{}) {
	logrus.Debug("event log: Error() called: ", args)
	lw.logger.Error(args...)
}

func (lw *LogWrapper) Errorf(format string, args ...interface{}) {
	logrus.Debug("event log: Errorf() called: ", format, args)
	lw.logger.Errorf(format, args...)
}

func logger(syslogTarget string) (types.EventLogger, error) {
	if syslogTarget == "" {
		return nil, nil
	}
	hook, err := logrusSyslog.NewSyslogHook("udp", syslogTarget, syslog.LOG_INFO, "longhorn-manager")
	if err != nil {
		return nil, errors.Wrapf(err, "unable to hook into syslog, syslogTarget '%s'", syslogTarget)
	}
	log := logrus.New()
	log.Formatter = &logrus.TextFormatter{ForceColors: true}
	log.Hooks.Add(hook)
	return log, nil
}

func Update(syslogTarget string) error {
	el, err := logger(syslogTarget)
	if err != nil {
		Set(nil)
		return err
	}
	Set(el)
	return nil
}
