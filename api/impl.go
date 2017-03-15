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

func DoThenGetVol(f func(name string) error, getVolume func(name string) (*types.VolumeInfo, error)) func(name string) (*types.VolumeInfo, error) {
	return func(name string) (*types.VolumeInfo, error) {
		if err := f(name); err != nil {
			return nil, err
		}
		return getVolume(name)
	}
}

func HostIDFromAttachReq(req *http.Request) (string, error) {
	attachInput := AttachInput{}
	if err := json.NewDecoder(req.Body).Decode(&attachInput); err != nil {
		return "", errors.Wrap(err, "error parsing request body")
	}
	return attachInput.HostID, nil
}

type Fwd struct {
	sl    types.ServiceLocator
	proxy http.Handler
}

func (f *Fwd) Handler(getHostID HostIDFunc, h http.Handler) http.HandlerFunc {
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
