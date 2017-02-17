package manager

import (
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-orc/types"
	"io"
	"time"
)

var (
	MonitoringPeriod     = time.Second * 2
	MonitoringMaxRetries = 3
	CleanupPeriod        = time.Minute * 2
)

type monitorChan struct {
	monitorCh chan<- Event
	cleanupCh chan<- Event
}

func (mc *monitorChan) Close() error {
	defer func() {
		recover()
	}()
	defer close(mc.monitorCh)
	defer close(mc.cleanupCh)
	return nil
}

func Monitor(getController types.GetController) types.Monitor {
	return func(volume *types.VolumeInfo, man types.VolumeManager) io.Closer {
		monitorCh := make(chan Event)
		go monitor(getController(volume), volume, man, monitorCh)
		cleanupCh := make(chan Event)
		go cleanup(volume, man, cleanupCh)
		return &monitorChan{monitorCh, cleanupCh}
	}
}

func monitor(ctrl types.Controller, volume *types.VolumeInfo, man types.VolumeManager, ch chan Event) {
	ticker := NewTicker(MonitoringPeriod, ch)
	defer ticker.Start().Stop()
	<-ch
	failedAttempts := 0
	for range ch {
		if err := func() error {
			defer ticker.Stop().Start()
			if err := man.CheckController(ctrl, volume); err != nil {
				if err, ok := err.(ControllerError); ok {
					return errors.Wrapf(err.Cause(), "controller failed, volume '%s'", volume.Name)
				}
				if failedAttempts++; failedAttempts > MonitoringMaxRetries {
					return errors.Wrapf(err, "repeated errors checking volume '%s', giving up", volume.Name)
				}
				logrus.Warnf("%v", errors.Wrapf(err, "error checking volume '%s', going to retry", volume.Name))
				return nil
			}
			failedAttempts = 0
			return nil
		}(); err != nil {
			close(ch)
			logrus.Error(errors.Wrapf(err, "detaching volume"))
			if err := man.Detach(volume.Name); err != nil {
				logrus.Errorf("%+v", errors.Wrapf(err, "error detaching failed volume '%s'", volume.Name))
			}
		}
	}
}

func cleanup(volume *types.VolumeInfo, man types.VolumeManager, ch chan Event) {
	ticker := NewTicker(CleanupPeriod, ch)
	defer ticker.Start().Stop()
	<-ch
	for range ch {
		func() {
			defer ticker.Stop().Start()
			if err := man.Cleanup(volume); err != nil {
				logrus.Warnf("%v", errors.Wrapf(err, "error cleaning up volume '%s'", volume.Name))
			}
		}()
	}
}
