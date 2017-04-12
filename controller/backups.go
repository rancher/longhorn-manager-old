package controller

import (
	"bytes"
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
	"os/exec"
	"time"
)

func (c *controller) runBackup(backupTarget, snapName string) {
	c.Lock()
	defer c.Unlock()

	func() {
		c.backupStatusLock.Lock()
		defer c.backupStatusLock.Unlock()

		c.backupStatus = &types.BackupStatusInfo{
			InProgress:   true,
			Err:          nil,
			Snapshot:     snapName,
			BackupTarget: backupTarget,
			Started:      util.FormatTimeZ(time.Now()),
		}
	}()
	defer func() {
		c.backupStatusLock.Lock()
		defer c.backupStatusLock.Unlock()

		c.backupStatus.InProgress = false
	}()

	var stdout, stderr bytes.Buffer
	cmd := exec.Command("longhorn", "--url", c.url, "backup", "create", "--dest", backupTarget, snapName)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	func() {
		c.backupStatusLock.Lock()
		defer c.backupStatusLock.Unlock()

		c.backupStatus.Err = errors.Wrapf(err, "error creating backup for snapshot '%s', backupTarget '%s': %s",
			snapName, backupTarget, &stderr)
	}()
	if err == nil {
		logrus.Infof("completed backup: volume '%s', snapshot '%s', backupTarget '%s'", c.name, snapName, backupTarget)
	}
}

func (c *controller) BackupOps() types.VolumeBackupOps {
	return c
}

func (c *controller) StartBackup(snapName, backupTarget string) error {
	snap, err := c.Get(snapName)
	if err != nil {
		return errors.Wrapf(err, "error getting snapshot '%s', volume '%s'", snapName, c.name)
	}
	if snap == nil {
		return errors.Errorf("could not find snapshot '%s' to backup, volume '%s'", snapName, c.name)
	}
	go c.runBackup(backupTarget, snapName)
	return nil
}

func (c *controller) Backup(snapName, backupTarget string) error {
	snap, err := c.Get(snapName)
	if err != nil {
		return errors.Wrapf(err, "error getting snapshot '%s', volume '%s'", snapName, c.name)
	}
	if snap == nil {
		return errors.Errorf("could not find snapshot '%s' to backup, volume '%s'", snapName, c.name)
	}
	c.runBackup(backupTarget, snapName)

	c.backupStatusLock.Lock()
	defer c.backupStatusLock.Unlock()

	return c.backupStatus.Err
}

func (c *controller) LatestBackupStatus() *types.BackupStatusInfo {
	c.backupStatusLock.Lock()
	defer c.backupStatusLock.Unlock()

	return c.backupStatus
}

func (c *controller) Restore(backup string) error {
	if _, err := util.Execute("longhorn", "--url", c.url, "backup", "restore", backup); err != nil {
		return errors.Wrapf(err, "error restoring backup '%s'", backup)
	}
	return nil
}

func (c *controller) DeleteBackup(backup string) error {
	if _, err := util.Execute("longhorn", "--url", c.url, "backup", "rm", backup); err != nil {
		return errors.Wrapf(err, "error deleting backup '%s'", backup)
	}
	return nil
}
