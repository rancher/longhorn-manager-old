package eventlog

import (
	"github.com/Sirupsen/logrus"
	"testing"
)

func TestLogWrapper(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetFormatter(&logrus.TextFormatter{ForceColors: true})

	lw := nilWrapper
	lw.Infof("lw nil, message: '%s'", "arg1")

	lw = &LogWrapper{logrus.StandardLogger()}
	lw.Infof("lw standard, message: '%s'", "arg1")
}
