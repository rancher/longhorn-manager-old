package api

import (
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/rancher/longhorn-manager/eventlog"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
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
		apiContext.Write(&Empty{})
		return nil
	}

	apiContext.Write(toVolumeResource(v, apiContext))
	return nil
}

func (s *Server) UpdateRecurring(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["name"]

	var recurring RecurringInput
	if err := apiContext.Read(&recurring); err != nil {
		return errors.Wrapf(err, "unable to parse recurring schedule for update")
	}

	jobs := make([]*types.RecurringJob, len(recurring.Jobs))
	for i := range recurring.Jobs { // cannot use i, job here: &job would be the same pointer for every iteration
		jobs[i] = &recurring.Jobs[i]
	}

	if err := s.man.UpdateRecurring(id, jobs); err != nil {
		eventlog.Errorf("Error updating recurring jobs, volume '%s'", id)
		return errors.Wrapf(err, "unable to update volume recurring schedule")
	}
	eventlog.Infof("Updated recurring jobs, volume '%s'", id)

	return s.GetVolume(rw, req)
}

func (s *Server) BgTaskQueue(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	name := mux.Vars(req)["name"]

	controller, err := s.man.Controller(name)
	if err != nil {
		return errors.Wrapf(err, "unable to get VolumeBackupOps for volume '%s'", name)
	}

	apiContext.Write(toBgTaskCollection(append(controller.LatestBgTasks(), controller.BgTaskQueue().List()...)))
	return nil
}

func (s *Server) DeleteVolume(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	if err := s.man.Delete(id); err != nil {
		eventlog.Errorf("Error deleting volume '%s'", id)
		return errors.Wrap(err, "unable to delete volume")
	}
	eventlog.Infof("Deleted volume '%s'", id)

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
		eventlog.Errorf("Error creating volume '%s'", volume.Name)
		return errors.Wrap(err, "unable to create volume")
	}
	eventlog.Infof("Created volume '%s'", volume.Name)

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
		eventlog.Errorf("Error attaching volume '%s' to host '%s'", id, s.sl.GetCurrentHostID())
		return errors.Wrap(err, "unable to attach volume")
	}
	eventlog.Infof("Attached volume '%s' to host '%s'", id, s.sl.GetCurrentHostID())

	return s.GetVolume(rw, req)
}

func (s *Server) DetachVolume(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	if err := s.man.Detach(id); err != nil {
		eventlog.Errorf("Error detaching volume '%s'", id)
		return errors.Wrap(err, "unable to detach volume")
	}
	eventlog.Infof("Detached volume '%s'", id)

	return s.GetVolume(rw, req)
}

func (s *Server) ReplicaRemove(rw http.ResponseWriter, req *http.Request) error {
	var input ReplicaRemoveInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error read replicaRemoveInput")
	}

	id := mux.Vars(req)["name"]

	if err := s.man.ReplicaRemove(id, input.Name); err != nil {
		eventlog.Errorf("Error removing replica '%s', volume '%s'", input.Name, id)
		return errors.Wrap(err, "unable to remove replica")
	}
	eventlog.Infof("Removed replica '%s', volume '%s'", input.Name, id)

	return s.GetVolume(rw, req)
}
