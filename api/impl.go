package api

import (
	"encoding/json"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
	"io"
	"net/http"
	"net/http/httputil"
)

type HostIDFunc func(req *http.Request) (string, error)

func dataFromReq(body io.ReadCloser) (map[string]interface{}, error) {
	data := map[string]interface{}{}
	if err := json.NewDecoder(body).Decode(&data); err != nil {
		return nil, errors.Wrap(err, "could not parse req body")
	}
	return data, nil
}

func HostIDFromAttachReq(req *http.Request) (string, error) {
	attachInput := AttachInput{}
	if err := json.NewDecoder(req.Body).Decode(&attachInput); err != nil {
		return "", errors.Wrap(err, "error parsing request body")
	}
	return attachInput.HostID, nil
}

func HostIDFromVolume(man types.VolumeManager) func(req *http.Request) (string, error) {
	return func(req *http.Request) (string, error) {
		name := mux.Vars(req)["name"]
		volume, err := man.Get(name)
		if err != nil {
			return "", errors.Wrapf(err, "error getting volume '%s'", name)
		}
		if volume == nil || volume.Controller == nil || !volume.Controller.Running {
			return "", nil
		}
		return volume.Controller.HostID, nil
	}
}

type Fwd struct {
	sl    types.ServiceLocator
	proxy http.Handler
}

func (f *Fwd) Handler(getHostID HostIDFunc, h HandleFuncWithError) HandleFuncWithError {
	return func(w http.ResponseWriter, req *http.Request) error {
		hostID, err := getHostID(util.CopyReq(req))
		if err != nil {
			return errors.Wrap(err, "fail to get host ID")
		}
		if hostID == "" {
			return errors.Wrap(err, "host ID is none")
		}
		if hostID != f.sl.GetCurrentHostID() {
			targetHost, err := f.sl.GetAddress(hostID)
			if err != nil {
				return errors.Wrapf(err, "cannot find host %v", hostID)
			}
			targetHost = fmt.Sprintf("%v:%v", targetHost, Port)
			if targetHost != req.Host {
				req.Host = targetHost
				req.URL.Host = targetHost
				req.URL.Scheme = "http"
				logrus.Debugf("Forwarding request to %v", targetHost)
				f.proxy.ServeHTTP(w, req)
				return nil
			}
		}
		return h(w, req)
	}
}

func Proxy() http.Handler {
	return &httputil.ReverseProxy{Director: func(r *http.Request) {}}
}

type SnapshotHandlers struct {
	man types.VolumeManager
}

func (sh *SnapshotHandlers) Create(w http.ResponseWriter, req *http.Request) error {
	volName := mux.Vars(req)["name"]

	snapshots, err := sh.man.VolumeSnapshots(volName)
	if err != nil {
		return errors.Wrapf(err, "error getting VolumeSnapshots for volume '%s'", volName)
	}

	data, err := dataFromReq(req.Body)
	if err != nil {
		return err
	}
	s0, err := fromSnapshotResMap(data)
	if err != nil {
		return err
	}
	snapName, err := snapshots.Create(s0.Name)
	logrus.Debugf("created snapshot '%s'", snapName)
	if err != nil {
		return errors.Wrapf(err, "error creating snapshot '%s', for volume '%s'", s0.Name, volName)
	}
	snap, err := snapshots.Get(snapName)
	if err != nil {
		return errors.Wrapf(err, "error getting snapshot '%s', for volume '%s'", snapName, volName)
	}
	if snap == nil {
		return errors.Errorf("not found just created snapshot '%s', for volume '%s'", snapName, volName)
	}
	logrus.Debugf("success: created snapshot '%s' for volume '%s'", snapName, volName)
	api.GetApiContext(req).Write(toSnapshotResource(snap))
	return nil
}

func (sh *SnapshotHandlers) List(w http.ResponseWriter, req *http.Request) error {
	volName := mux.Vars(req)["name"]

	snapshots, err := sh.man.VolumeSnapshots(volName)
	if err != nil {
		return errors.Wrapf(err, "error getting VolumeSnapshots for volume '%s'", volName)
	}

	snapList, err := snapshots.List()
	if err != nil {
		return errors.Wrapf(err, "error listing snapshots, for volume '%+v'", volName)
	}
	logrus.Debugf("success: listed snapshots for volume '%s'", volName)
	api.GetApiContext(req).Write(toSnapshotCollection(snapList))
	return nil
}

func (sh *SnapshotHandlers) Get(w http.ResponseWriter, req *http.Request) error {
	volName := mux.Vars(req)["name"]
	snapName := mux.Vars(req)["snapName"]

	snapshots, err := sh.man.VolumeSnapshots(volName)
	if err != nil {
		return errors.Wrapf(err, "error getting VolumeSnapshots for volume '%s'", volName)
	}

	snap, err := snapshots.Get(snapName)
	if err != nil {
		return errors.Wrapf(err, "error getting snapshot '%s', for volume '%s'", snapName, volName)
	}
	if snap == nil {
		return errors.Errorf("not found snapshot '%s', for volume '%s'", snapName, volName)
	}
	logrus.Debugf("success: got snapshot '%s' for volume '%s'", snap.Name, volName)
	api.GetApiContext(req).Write(toSnapshotResource(snap))
	return nil
}

func (sh *SnapshotHandlers) Delete(w http.ResponseWriter, req *http.Request) error {
	volName := mux.Vars(req)["name"]
	snapName := mux.Vars(req)["snapName"]

	snapshots, err := sh.man.VolumeSnapshots(volName)
	if err != nil {
		return errors.Wrapf(err, "error getting VolumeSnapshots for volume '%s'", volName)
	}

	if err := snapshots.Delete(snapName); err != nil {
		return errors.Wrapf(err, "error deleting snapshot '%+v', for volume '%+v'", snapName, volName)
	}
	logrus.Debugf("success: deleted snapshot '%s' for volume '%s'", snapName, volName)
	api.GetApiContext(req).Write(&Empty{})
	return nil
}

func (sh *SnapshotHandlers) Revert(w http.ResponseWriter, req *http.Request) error {
	volName := mux.Vars(req)["name"]
	snapName := mux.Vars(req)["snapName"]

	snapshots, err := sh.man.VolumeSnapshots(volName)
	if err != nil {
		return errors.Wrapf(err, "error getting VolumeSnapshots for volume '%s'", volName)
	}

	if err := snapshots.Revert(snapName); err != nil {
		return errors.Wrapf(err, "error reverting to snapshot '%+v', for volume '%+v'", snapName, volName)
	}
	logrus.Debugf("success: reverted to snapshot '%s' for volume '%s'", snapName, volName)
	api.GetApiContext(req).Write(&Empty{})
	return nil
}

func (sh *SnapshotHandlers) Backup(w http.ResponseWriter, req *http.Request) error {
	volName := mux.Vars(req)["name"]
	snapName := mux.Vars(req)["snapName"]

	backupTarget := sh.man.Settings().GetSettings().BackupTarget
	if backupTarget == "" {
		return errors.New("cannot backup: backupTarget not set")
	}

	backups, err := sh.man.VolumeBackups(volName)
	if err != nil {
		return errors.Wrapf(err, "error getting VolumeBackups for volume '%s'", volName)
	}

	if err := backups.Backup(snapName, backupTarget); err != nil {
		return errors.Wrapf(err, "error creating backup: snapshot '%s', volume '%s', dest '%s'", snapName, volName, backupTarget)
	}
	logrus.Debugf("success: started backup: snapshot '%s', volume '%s', dest '%s'", snapName, volName, backupTarget)
	api.GetApiContext(req).Write(&Empty{})
	return nil
}

type BackupsHandlers struct {
	man types.VolumeManager
}

func (bh *BackupsHandlers) List(w http.ResponseWriter, req *http.Request) {
	volName := mux.Vars(req)["volName"]

	backupTarget := bh.man.Settings().GetSettings().BackupTarget
	backups := bh.man.Backups(backupTarget)

	bs, err := backups.List(volName)
	if err != nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "error listing backups, backupTarget '%s', volume '%s'", backupTarget, volName))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	logrus.Debugf("success: list backups, volume '%s', backupTarget '%s'", volName, backupTarget)
	api.GetApiContext(req).Write(toBackupCollection(bs))
}

func backupURL(backupTarget, backupName, volName string) string {
	return fmt.Sprintf("%s?backup=%s&volume=%s", backupTarget, backupName, volName)
}

func (bh *BackupsHandlers) Get(w http.ResponseWriter, req *http.Request) {
	volName := mux.Vars(req)["volName"]
	backupName := mux.Vars(req)["backupName"]

	backupTarget := bh.man.Settings().GetSettings().BackupTarget
	backups := bh.man.Backups(backupTarget)

	url := backupURL(backupTarget, backupName, volName)
	backup, err := backups.Get(url)
	if err != nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "error getting backup '%s'", url))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if backup == nil {
		logrus.Warnf("not found: backup '%s'", url)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	logrus.Debugf("success: got backup '%s'", url)
	api.GetApiContext(req).Write(toBackupResource(backup))
}

func (bh *BackupsHandlers) Delete(w http.ResponseWriter, req *http.Request) {
	volName := mux.Vars(req)["volName"]
	backupName := mux.Vars(req)["backupName"]

	backupTarget := bh.man.Settings().GetSettings().BackupTarget
	backups := bh.man.Backups(backupTarget)

	url := backupURL(backupTarget, backupName, volName)
	if err := backups.Delete(url); err != nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "error deleting backup '%s'", url))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	logrus.Debugf("success: removed backup '%s'", url)
	api.GetApiContext(req).Write(&Empty{})
}
