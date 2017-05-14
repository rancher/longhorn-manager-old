package controller

import (
	"bytes"
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-manager/eventlog"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
	"os/exec"
	"time"
)

func (c *controller) LatestBgTasks() []*types.BgTask {
	c.bgTaskLock.Lock()
	defer c.bgTaskLock.Unlock()

	r := []*types.BgTask{}

	if c.lastRunBgTask != nil {
		r = append(r, c.lastRunBgTask)
	}
	if c.runningBgTask != nil {
		r = append(r, c.runningBgTask)
	}

	return r
}

func (c *controller) BgTaskQueue() types.TaskQueue {
	return c.bgTaskQueue
}

func (c *controller) runBgTasks() {
	for {
		t := c.bgTaskQueue.Take()
		if t == nil {
			break
		}
		c.runTask(t)
	}
}

func (c *controller) runTask(t *types.BgTask) {
	t.Started = util.FormatTimeZ(time.Now())

	func() {
		c.bgTaskLock.Lock()
		defer c.bgTaskLock.Unlock()

		c.runningBgTask = t
	}()
	var err error
	defer func() {
		c.bgTaskLock.Lock()
		defer c.bgTaskLock.Unlock()

		c.lastRunBgTask = c.runningBgTask
		c.runningBgTask = nil
		c.lastRunBgTask.Finished = util.FormatTimeZ(time.Now())
		c.lastRunBgTask.Err = err
	}()

	switch task := t.Task.(type) {
	case *types.BackupBgTask:
		err = c.runBackup(task)
	default:
		err = errors.Errorf("unknown task type: %#v", task)
	}
	if err != nil {
		eventlog.Errorf("Error running background task %+v", t.Task)
		logrus.Errorf("%+v", err)
	} else {
		eventlog.Infof("Completed background task %+v", t.Task)
	}
}

func (c *controller) runBackup(t *types.BackupBgTask) error {
	if t.CleanupHook != nil {
		defer func() {
			if err := t.CleanupHook(); err != nil {
				logrus.Errorf("%+v", errors.Wrapf(err, "error running cleanup after BackupBgTask, snapshot '%s'", t.Snapshot))
			}
		}()
	}

	var stdout, stderr bytes.Buffer
	cmd := exec.Command("longhorn", "--url", c.url, "backup", "create", "--dest", t.BackupTarget, t.Snapshot)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	if err == nil {
		logrus.Infof("completed backup: volume '%s', snapshot '%s', backupTarget '%s'", c.name, t.Snapshot, t.BackupTarget)
	}
	return errors.Wrapf(err, "error creating backup for snapshot '%s', backupTarget '%s': %s", t.Snapshot, t.BackupTarget, &stderr)
}
