package docker

import (
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	dTypes "github.com/docker/docker/api/types"
	dContainer "github.com/docker/docker/api/types/container"
	dNat "github.com/docker/go-connections/nat"

	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
)

const (
	OrcName = "docker"
)

var (
	ContainerStopTimeout = 1 * time.Minute
	WaitDeviceTimeout    = 30 //seconds
	WaitAPITimeout       = 30 //seconds
)

type dockerScheduleData struct {
	InstanceName     string
	VolumeName       string
	VolumeSize       string
	LonghornImage    string
	ReplicaAddresses []string
}

func (d *dockerOrc) ProcessSchedule(item *types.ScheduleItem) (*types.InstanceInfo, error) {
	var (
		data     dockerScheduleData
		instance *types.InstanceInfo
		err      error
	)

	if item.Data.Orchestrator != OrcName {
		return nil, errors.Errorf("received request for the wrong orchestrator %v", item.Data.Orchestrator)
	}
	if len(item.Data.Data) != 0 {
		if err := json.Unmarshal(item.Data.Data, &data); err != nil {
			return nil, errors.Wrap(err, "fail to parse schedule data")
		}
	}
	if item.Instance.ID == "" {
		return nil, errors.Errorf("empty instance ID")
	}
	input := &types.InstanceInfo{
		ID:         item.Instance.ID,
		HostID:     item.Instance.HostID,
		Type:       item.Instance.Type,
		VolumeName: item.Instance.VolumeName,
	}
	switch item.Action {
	case types.ScheduleActionCreateController:
		instance, err = d.createController(&data)
	case types.ScheduleActionCreateReplica:
		instance, err = d.createReplica(&data)
	case types.ScheduleActionStartInstance:
		instance, err = d.startInstance(input)
	case types.ScheduleActionStopInstance:
		instance, err = d.stopInstance(input)
	case types.ScheduleActionDeleteInstance:
		instance, err = d.removeInstance(input)
	default:
		return nil, errors.Errorf("cannot find specified action %v", item.Action)
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to process schedule")
	}
	if item.Action == types.ScheduleActionDeleteInstance {
		err = d.removeInstanceMetadata(instance)
	} else {
		err = d.updateInstanceMetadata(instance)
	}
	if err != nil {
		if item.Action == types.ScheduleActionCreateController ||
			item.Action == types.ScheduleActionCreateReplica {
			logrus.Warnf("failed to update instance metadata for %+v, cleaning up", instance)
			d.removeInstance(instance)
		}

		return nil, errors.Wrapf(err, "failed to update instance metadata for %+v", instance)
	}
	return instance, nil
}

func (d *dockerOrc) CreateController(volumeName, controllerName string, replicas map[string]*types.ReplicaInfo) (*types.ControllerInfo, error) {
	replicaNames := []string{}
	for name := range replicas {
		replicaNames = append(replicaNames, name)
	}
	data, err := d.prepareCreateController(volumeName, controllerName, replicaNames)
	if err != nil {
		return nil, errors.Wrapf(err, "Fail to create controller for %v", volumeName)
	}
	schedule := &types.ScheduleItem{
		Action: types.ScheduleActionCreateController,
		Instance: types.ScheduleInstance{
			ID:         controllerName,
			HostID:     d.GetCurrentHostID(),
			Type:       types.InstanceTypeController,
			VolumeName: volumeName,
		},
		Data: *data,
	}
	instance, err := d.scheduler.Schedule(schedule)
	if err != nil {
		return nil, errors.Wrapf(err, "Fail to create controller for %v", volumeName)
	}
	return &types.ControllerInfo{
		InstanceInfo: *instance,
	}, nil
}

func (d *dockerOrc) prepareCreateController(volumeName, controllerName string, replicaNames []string) (*types.ScheduleData, error) {
	volume, err := d.getVolume(volumeName)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create controller")
	}
	if volume == nil {
		return nil, errors.Wrapf(err, "unable to find volume %v", volumeName)
	}

	data := &dockerScheduleData{
		InstanceName:     controllerName,
		VolumeName:       volumeName,
		LonghornImage:    volume.LonghornImage,
		ReplicaAddresses: []string{},
	}
	for _, name := range replicaNames {
		replica := volume.Replicas[name]
		if replica == nil {
			return nil, errors.Errorf("cannot find replica %v", name)
		}
		data.ReplicaAddresses = append(data.ReplicaAddresses, "tcp://"+replica.Address+":9502")
	}

	bData, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to marshall %+v", data)
	}
	return &types.ScheduleData{
		Orchestrator: OrcName,
		Data:         bData,
	}, nil
}

func (d *dockerOrc) createController(data *dockerScheduleData) (instance *types.InstanceInfo, err error) {
	cmd := []string{
		"launch", "controller",
		"--listen", "0.0.0.0:9501",
		"--frontend", "tgt",
	}
	for _, address := range data.ReplicaAddresses {
		cmd = append(cmd, "--replica", address)
	}
	cmd = append(cmd, data.VolumeName)

	createBody, err := d.cli.ContainerCreate(context.Background(),
		&dContainer.Config{
			Image: data.LonghornImage,
			Cmd:   cmd,
		},
		&dContainer.HostConfig{
			Binds: []string{
				"/dev:/host/dev",
				"/proc:/host/proc",
			},
			Privileged: true,
		}, nil, data.InstanceName)
	if err != nil {
		return nil, errors.Wrap(err, "fail to create controller container")
	}

	defer func() {
		if err != nil && instance != nil {
			logrus.Errorf("fail to start controller %v of %v, cleaning up: %v",
				data.InstanceName, data.VolumeName, err)
			d.removeInstance(instance)
			instance = nil
		}
	}()

	instance = &types.InstanceInfo{
		ID:         createBody.ID,
		HostID:     d.GetCurrentHostID(),
		Name:       data.InstanceName,
		Type:       types.InstanceTypeController,
		VolumeName: data.VolumeName,
	}
	instance, err = d.startInstance(instance)
	if err != nil {
		return instance, errors.Wrap(err, "fail to start controller container")
	}

	//FIXME different address format for controller
	instance.Address = "http://" + instance.Address + ":9501"

	url := instance.Address + "/v1"
	if err := util.WaitForAPI(url, WaitAPITimeout); err != nil {
		return instance, errors.Wrapf(err, "fail to wait for api endpoint at %v", url)
	}

	if err := util.WaitForDevice(d.getDeviceName(data.VolumeName), WaitDeviceTimeout); err != nil {
		return instance, errors.Wrapf(err, "fail to create controller for %v", instance.VolumeName)
	}

	return instance, nil
}

func (d *dockerOrc) getDeviceName(volumeName string) string {
	return filepath.Join("/dev/longhorn/", volumeName)
}

func (d *dockerOrc) CreateReplica(volumeName, replicaName string) (*types.ReplicaInfo, error) {
	data, err := d.prepareCreateReplica(volumeName, replicaName)
	if err != nil {
		return nil, errors.Wrapf(err, "Fail to create replica for %v", volumeName)
	}
	schedule := &types.ScheduleItem{
		Action: types.ScheduleActionCreateReplica,
		Instance: types.ScheduleInstance{
			ID:         replicaName,
			Type:       types.InstanceTypeReplica,
			VolumeName: volumeName,
		},
		Data: *data,
	}
	instance, err := d.scheduler.Schedule(schedule)
	if err != nil {
		return nil, errors.Wrapf(err, "Fail to create replica for %v", volumeName)
	}
	return &types.ReplicaInfo{
		InstanceInfo: *instance,
	}, nil
}

func (d *dockerOrc) prepareCreateReplica(volumeName, replicaName string) (*types.ScheduleData, error) {
	volume, err := d.getVolume(volumeName)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create replica")
	}
	if volume == nil {
		return nil, errors.Wrapf(err, "unable to find volume %v", volumeName)
	}
	if volume.Size == 0 {
		return nil, errors.Wrap(err, "invalid volume size 0")
	}
	data := &dockerScheduleData{
		VolumeName:    volume.Name,
		VolumeSize:    strconv.FormatInt(volume.Size, 10),
		InstanceName:  replicaName,
		LonghornImage: volume.LonghornImage,
	}
	bData, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to marshall %+v", data)
	}
	return &types.ScheduleData{
		Orchestrator: OrcName,
		Data:         bData,
	}, nil
}

func (d *dockerOrc) createReplica(data *dockerScheduleData) (instance *types.InstanceInfo, err error) {
	cmd := []string{
		"launch", "replica",
		"--listen", "0.0.0.0:9502",
		"--size", data.VolumeSize,
		"/volume",
	}
	createBody, err := d.cli.ContainerCreate(context.Background(),
		&dContainer.Config{
			ExposedPorts: dNat.PortSet{
				"9502-9504": struct{}{},
			},
			Image: data.LonghornImage,
			Volumes: map[string]struct{}{
				"/volume": {},
			},
			Cmd: cmd,
		},
		&dContainer.HostConfig{
			Privileged: true,
		}, nil, data.InstanceName)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to create replica for %v", data.VolumeName)
	}

	instance = &types.InstanceInfo{
		ID:         createBody.ID,
		HostID:     d.GetCurrentHostID(),
		Name:       data.InstanceName,
		Type:       types.InstanceTypeReplica,
		VolumeName: data.VolumeName,
	}
	instance, err = d.startInstance(instance)
	if err != nil {
		logrus.Errorf("fail to start replica %v of %v, cleaning up: %v", data.InstanceName, data.VolumeName, err)
		d.removeInstance(instance)
		return nil, errors.Wrapf(err, "fail to create replica for %v", instance.VolumeName)
	}

	return instance, nil
}

func (d *dockerOrc) refreshInstanceInfo(instance *types.InstanceInfo) (*types.InstanceInfo, error) {
	inspectJSON, err := d.cli.ContainerInspect(context.Background(), instance.ID)
	if err != nil {
		return nil, errors.Wrap(err, "fail to inspect replica container")
	}
	return &types.InstanceInfo{
		// It's weird that Docker put a forward slash to the container name
		// So it become "/replica-1"
		ID:         inspectJSON.ID,
		Type:       instance.Type,
		Name:       strings.TrimPrefix(inspectJSON.Name, "/"),
		HostID:     d.GetCurrentHostID(),
		Address:    inspectJSON.NetworkSettings.IPAddress,
		Running:    inspectJSON.State.Running,
		VolumeName: instance.VolumeName,
	}, nil
}

func getScheduleInstanceFromInstance(instance *types.InstanceInfo) (*types.ScheduleInstance, error) {
	if instance.ID == "" || instance.HostID == "" ||
		instance.Type == types.InstanceTypeNone ||
		instance.VolumeName == "" {
		return nil, errors.Errorf("Invalid instance info for schedule %+v", instance)
	}

	return &types.ScheduleInstance{
		ID:         instance.ID,
		Type:       instance.Type,
		HostID:     instance.HostID,
		VolumeName: instance.VolumeName,
	}, nil
}

func (d *dockerOrc) StartInstance(instance *types.InstanceInfo) error {
	si, err := getScheduleInstanceFromInstance(instance)
	if err != nil {
		return errors.Wrap(err, "fail to start instance")
	}
	schedule := &types.ScheduleItem{
		Action:   types.ScheduleActionStartInstance,
		Instance: *si,
		Data: types.ScheduleData{
			Orchestrator: OrcName,
		},
	}
	if _, err := d.scheduler.Schedule(schedule); err != nil {
		return errors.Wrapf(err, "Fail to start instance %v", instance.ID)
	}
	return nil
}

func (d *dockerOrc) startInstance(instance *types.InstanceInfo) (*types.InstanceInfo, error) {
	if err := d.startContainer(instance.ID); err != nil {
		return nil, errors.Wrapf(err, "fail to start instance '%v' type %v", instance.ID, instance.Type)
	}
	return d.refreshInstanceInfo(instance)
}

func (d *dockerOrc) startContainer(id string) error {
	return d.cli.ContainerStart(context.Background(), id, dTypes.ContainerStartOptions{})
}

func (d *dockerOrc) StopInstance(instance *types.InstanceInfo) error {
	si, err := getScheduleInstanceFromInstance(instance)
	if err != nil {
		return errors.Wrap(err, "fail to stop instance")
	}
	schedule := &types.ScheduleItem{
		Action:   types.ScheduleActionStopInstance,
		Instance: *si,
		Data: types.ScheduleData{
			Orchestrator: OrcName,
		},
	}
	if _, err := d.scheduler.Schedule(schedule); err != nil {
		return errors.Wrapf(err, "Fail to stop instance %v", instance.ID)
	}
	return nil
}

func (d *dockerOrc) stopInstance(instance *types.InstanceInfo) (*types.InstanceInfo, error) {
	if err := d.cli.ContainerStop(context.Background(),
		instance.ID, &ContainerStopTimeout); err != nil {
		return nil, errors.Wrapf(err, "fail to start instance '%v'", instance.ID)
	}
	return d.refreshInstanceInfo(instance)
}

func (d *dockerOrc) stopContainer(id string) error {
	return d.cli.ContainerStop(context.Background(), id, &ContainerStopTimeout)
}

func (d *dockerOrc) RemoveInstance(instance *types.InstanceInfo) error {
	si, err := getScheduleInstanceFromInstance(instance)
	if err != nil {
		return errors.Wrap(err, "fail to remove instance")
	}
	schedule := &types.ScheduleItem{
		Action:   types.ScheduleActionDeleteInstance,
		Instance: *si,
		Data: types.ScheduleData{
			Orchestrator: OrcName,
		},
	}
	if _, err := d.scheduler.Schedule(schedule); err != nil {
		return errors.Wrapf(err, "Fail to remove instance %v", instance.ID)
	}
	return nil
}

func (d *dockerOrc) removeInstance(instance *types.InstanceInfo) (*types.InstanceInfo, error) {
	if err := d.removeContainer(instance.ID); err != nil {
		return nil, errors.Wrapf(err, "Fail to remove instance %v", instance.ID)
	}
	ret := &types.InstanceInfo{
		ID:         instance.ID,
		Name:       instance.Name,
		HostID:     instance.HostID,
		Type:       instance.Type,
		VolumeName: instance.VolumeName,
	}
	return ret, nil
}

func (d *dockerOrc) removeContainer(id string) error {
	return d.cli.ContainerRemove(context.Background(), id, dTypes.ContainerRemoveOptions{RemoveVolumes: true})
}

func (d *dockerOrc) updateInstanceMetadata(instance *types.InstanceInfo) (err error) {
	if instance.ID == "" ||
		instance.Name == "" ||
		instance.HostID == "" ||
		instance.Type == types.InstanceTypeNone ||
		instance.VolumeName == "" {
		return errors.Errorf("invalid instance to update metadata: %+v", instance)
	}

	volume, err := d.getVolume(instance.VolumeName)
	if err != nil {
		return errors.Wrapf(err, "fail to update instance metadata: %+v", instance)
	}
	if volume == nil {
		return errors.Errorf("fail to find volume %v", instance.VolumeName)
	}

	if instance.Type == types.InstanceTypeController {
		controller := volume.Controller
		if controller != nil && (controller.ID != instance.ID || controller.HostID != instance.HostID) {
			return errors.Errorf("unable to update instance metadata: metadata conflict: %+v %+v",
				controller, instance)
		}
		volume.Controller = &types.ControllerInfo{
			*instance,
		}
	} else if instance.Type == types.InstanceTypeReplica {
		replica := volume.Replicas[instance.Name]
		if replica != nil && (replica.ID != instance.ID || replica.HostID != instance.HostID) {
			return errors.Errorf("unable to update instance metadata: replica %v metadata conflict: %+v %+v",
				instance.Name, replica, instance)
		}
		if volume.Replicas == nil {
			volume.Replicas = make(map[string]*types.ReplicaInfo)
		}
		volume.Replicas[instance.Name] = replica
	}
	if err := d.setVolume(volume); err != nil {
		return errors.Wrap(err, "fail to update instance metadata")
	}

	return nil
}

func (d *dockerOrc) removeInstanceMetadata(instance *types.InstanceInfo) (err error) {
	if instance.ID == "" ||
		instance.HostID == "" ||
		instance.Type == types.InstanceTypeNone ||
		instance.VolumeName == "" {
		return errors.Errorf("invalid instance to update metadata for %+v", instance)
	}

	volume, err := d.getVolume(instance.VolumeName)
	if err != nil {
		return errors.Wrapf(err, "fail to update instance metadata for %+v", instance)
	}
	if volume == nil {
		return errors.Errorf("fail to find volume %v", instance.VolumeName)
	}

	if instance.Type == types.InstanceTypeController {
		controller := volume.Controller
		if controller == nil {
			return errors.Errorf("unable to remove instance metadata: unable to find controller for volume %v",
				instance.VolumeName)
		}
		if controller.ID != instance.ID || controller.HostID != instance.HostID {
			return errors.Errorf("unable to remove instance metadata: metadata conflict: %+v %+v",
				controller, instance)
		}
		volume.Controller = nil
	} else if instance.Type == types.InstanceTypeReplica {
		var replica *types.ReplicaInfo
		if instance.Name != "" {
			replica = volume.Replicas[instance.Name]
		} else {
			for _, v := range volume.Replicas {
				// In case we have same instance ID in different host
				if v.ID == instance.ID && v.HostID == instance.HostID {
					replica = v
					break
				}
			}
		}
		if replica == nil {
			return errors.Errorf("unable to remove instance metadata: unable to find replica as %+v",
				instance)
		}

		if replica.ID != instance.ID || replica.HostID != instance.HostID {
			return errors.Errorf("unable to remove instance metadata: metadata conflict: %+v %+v",
				replica, instance)
		}
		delete(volume.Replicas, replica.Name)
	}

	if err := d.setVolume(volume); err != nil {
		return errors.Wrap(err, "fail to remove instance metadata")
	}

	return nil
}
