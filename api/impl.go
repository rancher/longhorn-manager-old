package api

import (
	"encoding/json"
	"net/http"
	"net/http/httputil"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
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
		if hostID != "" && hostID != f.sl.GetCurrentHostID() {
			targetHost, err := f.sl.GetAddress(hostID)
			if err != nil {
				return errors.Wrapf(err, "cannot find host %v", hostID)
			}
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
