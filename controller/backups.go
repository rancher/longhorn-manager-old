package controller

import (
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-orc/types"
	"os/exec"
	"sync"
)

var (
	backupRequests = make(chan func() error, 100) // 10 is probably enough, so just in case

	currentBackupLock = &sync.Mutex{}
	currentBackup     *types.BackupInfo
)

func init() {
	go backupExecutor()
}

func backupExecutor() {
	for f := range backupRequests {
		func() {
			defer func() {
				if e := recover(); e != nil {
					logrus.Errorf("PANIC: %+v", e)
				} else {
					logrus.Debug("backupExecutor: recover() returned <nil>")
				}
			}()
			if err := f(); err != nil {
				logrus.Errorf("%+v", errors.Wrap(err, "Error creating a backup"))
			}
		}()
	}
}

func (c *controller) runBackup(backupTarget, snapName string) func() error {
	return func() error {
		currentBackupLock.Lock()
		currentBackup = &types.BackupInfo{VolumeName: c.name, SnapshotName: snapName, URL: backupTarget + "/INCOMPLETE"}
		currentBackupLock.Unlock()
		defer func() {
			currentBackupLock.Lock()
			currentBackup = nil
			currentBackupLock.Unlock()
		}()
		cmd := exec.Command("longhorn", "--url", c.url, "backup", "create", "--dest", backupTarget, snapName)
		err := cmd.Run()
		if err != nil {
			return errors.Wrapf(err, "error creating backup for snapshot '%s', backupTarget '%s'", snapName, backupTarget)
		}
		logrus.Infof("completed backup: volume '%s', snapshot '%s', backupTarget '%s'", c.name, snapName, backupTarget)
		return nil
	}
}

func (c *controller) Backups() types.VolumeBackupOps {
	return c
}

func (c *controller) Backup(snapName, backupTarget string) error {
	snap, err := c.Get(snapName)
	if err != nil {
		return errors.Wrapf(err, "error getting snapshot '%s', volume '%s'", snapName, c.name)
	}
	if snap == nil {
		return errors.Errorf("could not find snapshot '%s' to backup, volume '%s'", snapName, c.name)
	}
	backupRequests <- c.runBackup(backupTarget, snapName)
	return nil
}

func CurrentBackup() *types.BackupInfo {
	currentBackupLock.Lock()
	defer currentBackupLock.Unlock()
	return currentBackup
}

func (c *controller) Restore(backup string) error {
	cmd := exec.Command("longhorn", "--url", c.url, "backup", "restore", backup)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "error restoring backup '%s' \n%s", backup, string(bs))
	}
	return nil
}
