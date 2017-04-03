package api

import (
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
)

func (s *Server) ListVolume(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	resp := &client.GenericCollection{}

	volumes, err := s.man.List()
	if err != nil {
		return errors.Wrapf(err, "unable to list")
	}

	for _, v := range volumes {
		resp.Data = append(resp.Data, toVolumeResource(v, apiContext))
	}
	resp.ResourceType = "volume"
	resp.CreateTypes = map[string]string{
		"volume": apiContext.UrlBuilder.Collection("volume"),
	}
	apiContext.Write(resp)

	return nil
}

func (s *Server) GetVolume(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["name"]

	v, err := s.man.Get(id)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}

	if v == nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	apiContext.Write(toVolumeResource(v, apiContext))
	return nil
}

func (s *Server) DeleteVolume(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	if err := s.man.Delete(id); err != nil {
		return errors.Wrap(err, "unable to delete volume")
	}

	return nil
}

func (s *Server) CreateVolume(rw http.ResponseWriter, req *http.Request) error {
	var v Volume
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&v); err != nil {
		return err
	}

	volume, err := filterCreateVolumeInput(&v)
	if err != nil {
		return errors.Wrap(err, "unable to filter create volume input")
	}

	volumeResp, err := s.man.Create(volume)
	if err != nil {
		return errors.Wrap(err, "unable to create volume")
	}
	apiContext.Write(toVolumeResource(volumeResp, apiContext))
	return nil
}

func filterCreateVolumeInput(v *Volume) (*types.VolumeInfo, error) {
	size, err := util.ConvertSize(v.Size)
	if err != nil {
		return nil, errors.Wrapf(err, "error converting size '%s'", v.Size)
	}
	return &types.VolumeInfo{
		Name:                v.Name,
		Size:                util.RoundUpSize(size),
		BaseImage:           v.BaseImage,
		FromBackup:          v.FromBackup,
		NumberOfReplicas:    v.NumberOfReplicas,
		StaleReplicaTimeout: time.Duration(v.StaleReplicaTimeout) * time.Minute,
	}, nil
}

func (s *Server) AttachVolume(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	if err := s.man.Attach(id); err != nil {
		return errors.Wrap(err, "unable to attach volume")
	}

	return s.GetVolume(rw, req)
}

func (s *Server) DetachVolume(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	if err := s.man.Detach(id); err != nil {
		return errors.Wrap(err, "unable to detach volume")
	}

	return s.GetVolume(rw, req)
}
