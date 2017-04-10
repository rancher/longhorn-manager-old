package api

import (
	"net/http"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/longhorn-orc/types"
)

type SnapshotHandlers struct {
	man types.VolumeManager
}

func (sh *SnapshotHandlers) Create(w http.ResponseWriter, req *http.Request) error {
	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error read snapshotInput")
	}

	for k, v := range input.Labels {
		if strings.Contains(k, "=") || strings.Contains(v, "=") {
			return errors.New("labels cannot contain '='")
		}
	}

	volName := mux.Vars(req)["name"]
	if volName == "" {
		return errors.Errorf("volume name required")
	}

	snapOps, err := sh.man.SnapshotOps(volName)
	if err != nil {
		return errors.Wrapf(err, "error getting SnapshotOps for volume '%s'", volName)
	}
	snapName, err := snapOps.Create(input.Name, input.Labels)
	if err != nil {
		return errors.Wrapf(err, "error creating snapshot '%s', for volume '%s'", input.Name, volName)
	}
	logrus.Debugf("created snapshot '%s'", snapName)

	snap, err := snapOps.Get(snapName)
	if err != nil {
		return errors.Wrapf(err, "error getting snapshot '%s', for volume '%s'", snapName, volName)
	}
	if snap == nil {
		return errors.Errorf("not found just created snapshot '%s', for volume '%s'", snapName, volName)
	}
	logrus.Debugf("success: created snapshot '%s' for volume '%s'", snapName, volName)
	apiContext.Write(toSnapshotResource(snap))
	return nil
}

func (sh *SnapshotHandlers) List(w http.ResponseWriter, req *http.Request) error {
	volName := mux.Vars(req)["name"]
	if volName == "" {
		return errors.Errorf("volume name required")
	}

	snapOps, err := sh.man.SnapshotOps(volName)
	if err != nil {
		return errors.Wrapf(err, "error getting SnapshotOps for volume '%s'", volName)
	}

	snapList, err := snapOps.List()
	if err != nil {
		return errors.Wrapf(err, "error listing snapshots, for volume '%+v'", volName)
	}
	logrus.Debugf("success: listed snapshots for volume '%s'", volName)
	api.GetApiContext(req).Write(toSnapshotCollection(snapList))
	return nil
}

func (sh *SnapshotHandlers) Get(w http.ResponseWriter, req *http.Request) error {
	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error read snapshotInput")
	}
	if input.Name == "" {
		return errors.Errorf("empty snapshot name not allowed")
	}

	volName := mux.Vars(req)["name"]
	if volName == "" {
		return errors.Errorf("volume name required")
	}

	snapOps, err := sh.man.SnapshotOps(volName)
	if err != nil {
		return errors.Wrapf(err, "error getting SnapshotOps for volume '%s'", volName)
	}

	snap, err := snapOps.Get(input.Name)
	if err != nil {
		return errors.Wrapf(err, "error getting snapshot '%s', for volume '%s'", input.Name, volName)
	}
	if snap == nil {
		return errors.Errorf("not found snapshot '%s', for volume '%s'", input.Name, volName)
	}
	logrus.Debugf("success: got snapshot '%s' for volume '%s'", snap.Name, volName)
	api.GetApiContext(req).Write(toSnapshotResource(snap))
	return nil
}

func (sh *SnapshotHandlers) Delete(w http.ResponseWriter, req *http.Request) error {
	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error read snapshotInput")
	}
	if input.Name == "" {
		return errors.Errorf("empty snapshot name not allowed")
	}

	volName := mux.Vars(req)["name"]
	if volName == "" {
		return errors.Errorf("volume name required")
	}

	snapOps, err := sh.man.SnapshotOps(volName)
	if err != nil {
		return errors.Wrapf(err, "error getting SnapshotOps for volume '%s'", volName)
	}

	if err := snapOps.Delete(input.Name); err != nil {
		return errors.Wrapf(err, "error deleting snapshot '%+v', for volume '%+v'", input.Name, volName)
	}

	snap, err := snapOps.Get(input.Name)
	if err != nil {
		return errors.Wrapf(err, "error getting snapshot '%s', for volume '%s'", input.Name, volName)
	}
	if snap == nil {
		return errors.Errorf("not found snapshot '%s', for volume '%s'", input.Name, volName)
	}

	logrus.Debugf("success: deleted snapshot '%s' for volume '%s'", input.Name, volName)
	api.GetApiContext(req).Write(toSnapshotResource(snap))
	return nil
}

func (sh *SnapshotHandlers) Revert(w http.ResponseWriter, req *http.Request) error {
	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error read snapshotInput")
	}
	if input.Name == "" {
		return errors.Errorf("empty snapshot name not allowed")
	}

	volName := mux.Vars(req)["name"]
	if volName == "" {
		return errors.Errorf("volume name required")
	}

	snapOps, err := sh.man.SnapshotOps(volName)
	if err != nil {
		return errors.Wrapf(err, "error getting SnapshotOps for volume '%s'", volName)
	}

	if err := snapOps.Revert(input.Name); err != nil {
		return errors.Wrapf(err, "error reverting to snapshot '%+v', for volume '%+v'", input.Name, volName)
	}

	snap, err := snapOps.Get(input.Name)
	if err != nil {
		return errors.Wrapf(err, "error getting snapshot '%s', for volume '%s'", input.Name, volName)
	}
	if snap == nil {
		return errors.Errorf("not found snapshot '%s', for volume '%s'", input.Name, volName)
	}

	logrus.Debugf("success: reverted to snapshot '%s' for volume '%s'", input.Name, volName)
	api.GetApiContext(req).Write(toSnapshotResource(snap))
	return nil
}

func (sh *SnapshotHandlers) Backup(w http.ResponseWriter, req *http.Request) error {
	var input SnapshotInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error read snapshotInput")
	}
	if input.Name == "" {
		return errors.Errorf("empty snapshot name not allowed")
	}

	volName := mux.Vars(req)["name"]
	if volName == "" {
		return errors.Errorf("volume name required")
	}

	settings, err := sh.man.Settings().GetSettings()
	if err != nil || settings == nil {
		return errors.New("cannot backup: unable to read settings")
	}
	backupTarget := settings.BackupTarget
	if backupTarget == "" {
		return errors.New("cannot backup: backupTarget not set")
	}

	backups, err := sh.man.VolumeBackupOps(volName)
	if err != nil {
		return errors.Wrapf(err, "error getting VolumeBackupOps for volume '%s'", volName)
	}

	if err := backups.StartBackup(input.Name, backupTarget); err != nil {
		return errors.Wrapf(err, "error creating backup: snapshot '%s', volume '%s', dest '%s'", input.Name, volName, backupTarget)
	}
	logrus.Debugf("success: started backup: snapshot '%s', volume '%s', dest '%s'", input.Name, volName, backupTarget)
	apiContext.Write(&Empty{})
	return nil
}

func (sh *SnapshotHandlers) Purge(w http.ResponseWriter, req *http.Request) error {
	volName := mux.Vars(req)["name"]
	if volName == "" {
		return errors.Errorf("volume name required")
	}

	snapOps, err := sh.man.SnapshotOps(volName)
	if err != nil {
		return errors.Wrapf(err, "error getting SnapshotOps for volume '%s'", volName)
	}

	if err := snapOps.Purge(); err != nil {
		return errors.Wrapf(err, "error purging snapshots, for volume '%+v'", volName)
	}
	logrus.Debugf("success: purge snapshots for volume '%s'", volName)
	api.GetApiContext(req).Write(&Empty{})
	return nil
}
