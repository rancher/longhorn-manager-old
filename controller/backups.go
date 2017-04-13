package controller

import (
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

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
	c.bgTaskQueue.Put(&types.BgTask{Task: &types.BackupBgTask{Snapshot: snapName, BackupTarget: backupTarget}})
	return nil
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
