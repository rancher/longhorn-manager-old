package api

import (
	"fmt"
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"

	"github.com/rancher/longhorn-orc/types"
)

type BackupsHandlers struct {
	man types.VolumeManager
}

func (bh *BackupsHandlers) ListVolume(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	backupTarget := bh.man.Settings().GetSettings().BackupTarget
	backups := bh.man.ManagerBackupOps(backupTarget)

	volumes, err := backups.ListVolumes()
	if err != nil {
		return errors.Wrapf(err, "error listing backups, backupTarget '%s'", backupTarget)
	}
	logrus.Debugf("success: list backup volumes, backupTarget '%s'", backupTarget)
	apiContext.Write(toBackupVolumeCollection(volumes, apiContext))
	return nil
}

func (bh *BackupsHandlers) GetVolume(w http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	volName := mux.Vars(req)["volName"]

	backupTarget := bh.man.Settings().GetSettings().BackupTarget
	backups := bh.man.ManagerBackupOps(backupTarget)

	bv, err := backups.GetVolume(volName)
	if err != nil {
		return errors.Wrapf(err, "error get backup volume, backupTarget '%s', volume '%s'", backupTarget, volName)
	}
	logrus.Debugf("success: get backup volume, volume '%s', backupTarget '%s'", volName, backupTarget)
	apiContext.Write(toBackupVolumeResource(bv, apiContext))
	return nil
}

func (bh *BackupsHandlers) List(w http.ResponseWriter, req *http.Request) error {
	volName := mux.Vars(req)["volName"]

	backupTarget := bh.man.Settings().GetSettings().BackupTarget
	backups := bh.man.ManagerBackupOps(backupTarget)

	bs, err := backups.List(volName)
	if err != nil {
		return errors.Wrapf(err, "error listing backups, backupTarget '%s', volume '%s'", backupTarget, volName)
	}
	logrus.Debugf("success: list backups, volume '%s', backupTarget '%s'", volName, backupTarget)
	api.GetApiContext(req).Write(toBackupCollection(bs))
	return nil
}

func backupURL(backupTarget, backupName, volName string) string {
	return fmt.Sprintf("%s?backup=%s&volume=%s", backupTarget, backupName, volName)
}

func (bh *BackupsHandlers) Get(w http.ResponseWriter, req *http.Request) error {
	var input BackupInput

	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}
	if input.Name == "" {
		return errors.Errorf("empty backup name is not allowed")
	}
	volName := mux.Vars(req)["volName"]

	backupTarget := bh.man.Settings().GetSettings().BackupTarget
	backups := bh.man.ManagerBackupOps(backupTarget)

	url := backupURL(backupTarget, input.Name, volName)
	backup, err := backups.Get(url)
	if err != nil {
		return errors.Wrapf(err, "error getting backup '%s'", url)
	}
	if backup == nil {
		logrus.Warnf("not found: backup '%s'", url)
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
	logrus.Debugf("success: got backup '%s'", url)
	apiContext.Write(toBackupResource(backup))
	return nil
}

func (bh *BackupsHandlers) Delete(w http.ResponseWriter, req *http.Request) error {
	var input BackupInput

	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}
	if input.Name == "" {
		return errors.Errorf("empty backup name is not allowed")
	}

	volName := mux.Vars(req)["volName"]

	backupTarget := bh.man.Settings().GetSettings().BackupTarget
	backups := bh.man.ManagerBackupOps(backupTarget)

	url := backupURL(backupTarget, input.Name, volName)
	if err := backups.Delete(url); err != nil {
		return errors.Wrapf(err, "error deleting backup '%s'", url)
	}
	logrus.Debugf("success: removed backup '%s'", url)
	apiContext.Write(&Empty{})
	return nil
}
