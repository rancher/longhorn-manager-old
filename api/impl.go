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

func Name2VolumeFunc(f func(name string) (*types.VolumeInfo, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		apiContext := api.GetApiContext(req)
		name := mux.Vars(req)["name"]

		volume, err := f(name)
		if err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error running '%+v', for name '%s'", f, name))
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if volume == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		logrus.Debugf("success: got volume '%+v' for name '%s'", volume, name)
		apiContext.Write(toVolumeResource(volume))
	}
}

func VolumeListFunc(f func() ([]*types.VolumeInfo, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		apiContext := api.GetApiContext(req)

		volumes, err := f()
		if err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error running '%+v'", f))
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if volumes == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		logrus.Debugf("success: got volumes '%+v'", volumes)
		apiContext.Write(toVolumeCollection(volumes))
	}
}

func dataFromReq(body io.ReadCloser) (map[string]interface{}, error) {
	data := map[string]interface{}{}
	if err := json.NewDecoder(body).Decode(&data); err != nil {
		return nil, errors.Wrap(err, "could not parse req body")
	}
	return data, nil
}

func Volume2VolumeFunc(f func(volume *types.VolumeInfo) (*types.VolumeInfo, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		apiContext := api.GetApiContext(req)

		data, err := dataFromReq(req.Body)
		if err != nil {
			logrus.Errorf("%+v", err)
			r.JSON(w, http.StatusBadRequest, err)
			return
		}
		volume0, err := fromVolumeResMap(data)
		if err != nil {
			logrus.Errorf("%+v", err)
			r.JSON(w, http.StatusBadRequest, err)
			return
		}
		volume, err := f(volume0)

		if err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error running '%+v', for volume '%+v'", f, volume0))
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if volume == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		logrus.Debugf("success: got volume '%+v' for volume '%+v'", volume, volume0)
		apiContext.Write(toVolumeResource(volume))
	}
}

func NameFunc(f func(name string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		apiContext := api.GetApiContext(req)
		name := mux.Vars(req)["name"]

		err := f(name)
		if err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error running '%+v', for name '%s'", f, name))
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		logrus.Debugf("success: done for name '%s'", name)
		apiContext.Write(&Empty{})
	}
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

func (f *Fwd) Handler(getHostID HostIDFunc, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		hostID, err := getHostID(util.CopyReq(req))
		if err != nil {
			logrus.Errorf("%+v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if hostID == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if !f.sl.IsLocal(hostID) {
			targetHost, err := f.sl.GetAddress(hostID)
			targetHost = targetHost + fmt.Sprintf(":%v", Port)
			if targetHost != req.Host {
				if err != nil {
					logrus.Errorf("%+v", errors.Wrapf(err, "error getting address for hostID='%s'", hostID))
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				req.Host = targetHost
				req.URL.Host = targetHost
				req.URL.Scheme = "http"
				f.proxy.ServeHTTP(w, req)
				return
			}
		}
		h.ServeHTTP(w, req)
	}
}

func Proxy() http.Handler {
	return &httputil.ReverseProxy{Director: func(r *http.Request) {}}
}

type SnapshotHandlers struct {
	man types.VolumeManager
}

func (sh *SnapshotHandlers) Create(w http.ResponseWriter, req *http.Request) {
	volName := mux.Vars(req)["name"]

	snapshots, err := sh.man.VolumeSnapshots(volName)
	if err != nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "error getting VolumeSnapshots for volume '%s'", volName))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	data, err := dataFromReq(req.Body)
	if err != nil {
		logrus.Errorf("%+v", err)
		r.JSON(w, http.StatusBadRequest, err)
		return
	}
	s0, err := fromSnapshotResMap(data)
	if err != nil {
		logrus.Errorf("%+v", err)
		r.JSON(w, http.StatusBadRequest, err)
		return
	}
	snapName, err := snapshots.Create(s0.Name)
	if err != nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "error creating snapshot '%+v', for volume '%+v'", s0.Name, volName))
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	snap, err := snapshots.Get(snapName)
	if err != nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "error getting snapshot '%+v', for volume '%+v'", snapName, volName))
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	if snap == nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "not found just created snapshot '%+v', for volume '%+v'", snapName, volName))
		w.WriteHeader(http.StatusNotFound)
		return
	}
	logrus.Debugf("success: created snapshot '%s' for volume '%s'", snapName, volName)
	api.GetApiContext(req).Write(toSnapshotResource(snap))
}

func (sh *SnapshotHandlers) List(w http.ResponseWriter, req *http.Request) {
	volName := mux.Vars(req)["name"]

	snapshots, err := sh.man.VolumeSnapshots(volName)
	if err != nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "error getting VolumeSnapshots for volume '%s'", volName))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	snapList, err := snapshots.List()
	if err != nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "error listing snapshots, for volume '%+v'", volName))
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	logrus.Debugf("success: listed snapshots for volume '%s'", volName)
	api.GetApiContext(req).Write(toSnapshotCollection(snapList))
}

func (sh *SnapshotHandlers) Get(w http.ResponseWriter, req *http.Request) {
	volName := mux.Vars(req)["name"]
	snapName := mux.Vars(req)["snapName"]

	snapshots, err := sh.man.VolumeSnapshots(volName)
	if err != nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "error getting VolumeSnapshots for volume '%s'", volName))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	snap, err := snapshots.Get(snapName)
	if err != nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "error getting snapshot '%+v', for volume '%+v'", snapName, volName))
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	if snap == nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "not found snapshot '%+v', for volume '%+v'", snapName, volName))
		w.WriteHeader(http.StatusNotFound)
		return
	}
	logrus.Debugf("success: got snapshot '%s' for volume '%s'", snap.Name, volName)
	api.GetApiContext(req).Write(toSnapshotResource(snap))
}

func (sh *SnapshotHandlers) Delete(w http.ResponseWriter, req *http.Request) {
	volName := mux.Vars(req)["name"]
	snapName := mux.Vars(req)["snapName"]

	snapshots, err := sh.man.VolumeSnapshots(volName)
	if err != nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "error getting VolumeSnapshots for volume '%s'", volName))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := snapshots.Delete(snapName); err != nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "error deleting snapshot '%+v', for volume '%+v'", snapName, volName))
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	logrus.Debugf("success: deleted snapshot '%s' for volume '%s'", snapName, volName)
	api.GetApiContext(req).Write(&Empty{})
}

func (sh *SnapshotHandlers) Revert(w http.ResponseWriter, req *http.Request) {
	volName := mux.Vars(req)["name"]
	snapName := mux.Vars(req)["snapName"]

	snapshots, err := sh.man.VolumeSnapshots(volName)
	if err != nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "error getting VolumeSnapshots for volume '%s'", volName))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := snapshots.Revert(snapName); err != nil {
		logrus.Errorf("%+v", errors.Wrapf(err, "error reverting to snapshot '%+v', for volume '%+v'", snapName, volName))
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	logrus.Debugf("success: reverted to snapshot '%s' for volume '%s'", snapName, volName)
	api.GetApiContext(req).Write(&Empty{})
}
