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
	"net/http"
	"net/http/httputil"
)

type HostIDFunc func(req *http.Request) (string, error)

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

type BackupsHandlers struct {
	man types.VolumeManager
}

func (bh *BackupsHandlers) List(w http.ResponseWriter, req *http.Request) {
	volName := mux.Vars(req)["volName"]

	backupTarget := bh.man.Settings().GetSettings().BackupTarget
	backups := bh.man.ManagerBackupOps(backupTarget)

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
	backups := bh.man.ManagerBackupOps(backupTarget)

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
	backups := bh.man.ManagerBackupOps(backupTarget)

	url := backupURL(backupTarget, backupName, volName)
	if err := backups.Delete(url); err != nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "error deleting backup '%s'", url))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	logrus.Debugf("success: removed backup '%s'", url)
	api.GetApiContext(req).Write(&Empty{})
}
