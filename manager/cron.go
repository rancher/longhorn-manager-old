package manager

import (
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
	"github.com/robfig/cron"
	"strings"
	"time"
)

var tasks = map[string]func(volume *types.VolumeInfo, ctrl types.Controller, name string) func(){
	types.SnapshotTask: snapshotTask,
	types.BackupTask:   backupTask,
}

type cronUpdate struct {
	Jobs []*types.RecurringJob
}

func CronUpdate(jobs []*types.RecurringJob) types.Event {
	return &cronUpdate{Jobs: jobs}
}

func doCron(volume *types.VolumeInfo, ctrl types.Controller, ch chan types.Event) {
	c := setJobs(volume, ctrl, volume.RecurringJobs)
	c.Start()
	defer func() {
		c.Stop()
	}()

	for e := range ch {
		switch e := e.(type) {
		case *cronUpdate:
			c.Stop()
			c = setJobs(volume, ctrl, e.Jobs)
			c.Start()
			logrus.Infof("restarted recurring jobs, volume '%s'", volume.Name)
		}
	}
}

func ValidateJobs(jobs []*types.RecurringJob) error {
	c := cron.NewWithLocation(time.UTC)
	for _, j := range jobs {
		if t := tasks[j.Task]; t != nil {
			if strings.TrimSpace(j.Name) != j.Name || j.Name == "" {
				return errors.Errorf("job name cannot be empty, start or end with whitespace: '%s'", j.Name)
			}
			if _, ok := tasks[j.Task]; !ok {
				return errors.Errorf("invalid task '%s'", j.Task)
			}
			if err := c.AddFunc(j.Cron, func() {}); err != nil {
				return errors.Wrap(err, "cron job validation error")
			}
		}
	}
	return nil
}

func setJobs(volume *types.VolumeInfo, ctrl types.Controller, jobs []*types.RecurringJob) *cron.Cron {
	c := cron.NewWithLocation(time.UTC)
	for _, j := range jobs {
		if t := tasks[j.Task]; t != nil {
			c.AddFunc(j.Cron, t(volume, ctrl, j.Name))
		}
	}
	return c
}

func snapName(name string) string {
	return name + "-" + util.FormatTimeZ(time.Now()) + "-" + util.RandomID()
}

func snapshotTask(volume *types.VolumeInfo, ctrl types.Controller, taskName string) func() {
	return func() {
		name := snapName(taskName)
		logrus.Infof("recurring job: snapshot '%s', volume '%s'", name, volume.Name)
		if _, err := ctrl.SnapshotOps().Create(name); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error running recurring job: snapshot '%s', volume '%s'", name, volume.Name))
		}
	}
}

func backupTask(volume *types.VolumeInfo, ctrl types.Controller, taskName string) func() {
	return func() {
		// TODO impl
	}
}
