package docker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
	"golang.org/x/net/context"

	eCli "github.com/coreos/etcd/client"

	dTypes "github.com/docker/docker/api/types"
	dContainer "github.com/docker/docker/api/types/container"
	dCli "github.com/docker/docker/client"
	dNat "github.com/docker/go-connections/nat"

	"github.com/rancher/longhorn-orc/api"
	"github.com/rancher/longhorn-orc/orch"
	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
)

const (
	OrcName = "docker"

	cfgDirectory = "/var/lib/rancher/longhorn/"
	hostUUIDFile = cfgDirectory + ".physical_host_uuid"
)

var (
	ContainerStopTimeout = 1 * time.Minute
	WaitDeviceTimeout    = 30 //seconds
	WaitAPITimeout       = 30 //seconds
)

type dockerOrc struct {
	Servers       []string //etcd servers
	Prefix        string   //prefix in k/v store
	LonghornImage string

	currentHost *types.HostInfo

	kapi eCli.KeysAPI
	cli  *dCli.Client
}

type dockerOrcConfig struct {
	servers []string
	prefix  string
	image   string
}

func New(c *cli.Context) (types.Orchestrator, error) {
	servers := c.StringSlice("etcd-servers")
	if len(servers) == 0 {
		return nil, fmt.Errorf("Unspecified etcd servers")
	}
	prefix := c.String("etcd-prefix")
	image := c.String(orch.LonghornImageParam)
	return newDocker(&dockerOrcConfig{
		servers: servers,
		prefix:  prefix,
		image:   image,
	})
}

func newDocker(cfg *dockerOrcConfig) (types.Orchestrator, error) {
	eCfg := eCli.Config{
		Endpoints:               cfg.servers,
		Transport:               eCli.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}

	etcdc, err := eCli.New(eCfg)
	if err != nil {
		return nil, err
	}

	docker := &dockerOrc{
		Servers:       cfg.servers,
		Prefix:        cfg.prefix,
		LonghornImage: cfg.image,

		kapi: eCli.NewKeysAPI(etcdc),
	}

	//Set Docker API to compatible with 1.12
	os.Setenv("DOCKER_API_VERSION", "1.24")
	docker.cli, err = dCli.NewEnvClient()
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to docker")
	}

	if _, err := docker.cli.ContainerList(context.Background(), dTypes.ContainerListOptions{}); err != nil {
		return nil, errors.Wrap(err, "cannot pass test to get container list")
	}

	ips, err := util.GetLocalIPs()
	if err != nil || len(ips) == 0 {
		return nil, fmt.Errorf("unable to get ip")
	}
	address := ips[0] + ":" + strconv.Itoa(api.DefaultPort)

	if err := docker.Register(address); err != nil {
		return nil, err
	}
	logrus.Info("Docker orchestrator is ready")
	return docker, nil
}

func getCurrentHost(address string) (*types.HostInfo, error) {
	var err error

	host := &types.HostInfo{
		Address: address,
	}
	host.Name, err = os.Hostname()
	if err != nil {
		return nil, err
	}

	uuid, err := ioutil.ReadFile(hostUUIDFile)
	if err == nil {
		host.UUID = string(uuid)
		return host, nil
	}

	// file doesn't exists, generate new UUID for the host
	host.UUID = util.UUID()
	if err := os.MkdirAll(cfgDirectory, os.ModeDir|0600); err != nil {
		return nil, fmt.Errorf("Fail to create configuration directory: %v", err)
	}
	if err := ioutil.WriteFile(hostUUIDFile, []byte(host.UUID), 0600); err != nil {
		return nil, fmt.Errorf("Fail to write host uuid file: %v", err)
	}
	return host, nil
}

func (d *dockerOrc) Register(address string) error {
	currentHost, err := getCurrentHost(address)
	if err != nil {
		return err
	}

	if err := d.setHost(currentHost); err != nil {
		return err
	}
	d.currentHost = currentHost
	return nil
}

func (d *dockerOrc) GetHost(id string) (*types.HostInfo, error) {
	return d.getHost(id)
}

func (d *dockerOrc) ListHosts() (map[string]*types.HostInfo, error) {
	return d.listHosts()
}

func (d *dockerOrc) GetCurrentHostID() string {
	return d.currentHost.UUID
}

func (d *dockerOrc) GetAddress(hostID string) (string, error) {
	if hostID == d.currentHost.UUID {
		return d.currentHost.Address, nil
	}
	host, err := d.GetHost(hostID)
	if err != nil {
		return "", err
	}
	return host.Address, nil
}

func (d *dockerOrc) CreateVolume(volume *types.VolumeInfo) (*types.VolumeInfo, error) {
	v, err := d.getVolume(volume.Name)
	if err == nil && v != nil {
		return nil, errors.Errorf("volume %v already exists %+v", volume.Name, v)
	}
	if err := d.setVolume(volume); err != nil {
		return nil, errors.Wrap(err, "fail to create new volume metadata")
	}
	return volume, nil
}

func (d *dockerOrc) DeleteVolume(volumeName string) error {
	return d.rmVolume(volumeName)
}

func (d *dockerOrc) GetVolume(volumeName string) (*types.VolumeInfo, error) {
	//TODO Update instances address and status
	return d.getVolume(volumeName)
}

func (d *dockerOrc) UpdateVolume(volume *types.VolumeInfo) error {
	v, err := d.getVolume(volume.Name)
	if err != nil {
		return errors.Errorf("cannot update volume %v because it doesn't exists %+v", volume.Name, v)
	}
	return d.setVolume(volume)
}

func (d *dockerOrc) ListVolumes() ([]*types.VolumeInfo, error) {
	return d.listVolumes()
}

func (d *dockerOrc) MarkBadReplica(volumeName string, replica *types.ReplicaInfo) error {
	return nil
}

type dockerScheduleData struct {
	InstanceID       string
	InstanceName     string
	VolumeName       string
	VolumeSize       string
	LonghornImage    string
	ReplicaAddresses []string
}

func (d *dockerOrc) ProcessSchedule(item *types.ScheduleItem) (*types.InstanceInfo, error) {
	var data dockerScheduleData

	if item.Data == nil {
		return nil, errors.Errorf("cannot find required item.Data %+v", item)
	}
	if item.Data.Orchestrator != OrcName {
		return nil, errors.Errorf("received request for the wrong orchestrator %v", item.Data.Orchestrator)
	}
	if err := json.Unmarshal(item.Data.Data, &data); err != nil {
		return nil, errors.Wrap(err, "fail to parse schedule data")
	}
	switch item.Action {
	case types.ScheduleActionCreateController:
		return d.createController(&data)
	case types.ScheduleActionCreateReplica:
		return d.createReplica(&data)
	case types.ScheduleActionStartInstance:
		return d.startInstance(data.InstanceID)
	case types.ScheduleActionStopInstance:
		return d.stopInstance(data.InstanceID)
	case types.ScheduleActionDeleteInstance:
		return d.removeInstance(data.InstanceID)
	}
	return nil, errors.Errorf("Cannot find specified action %v", item.Action)
}

func (d *dockerOrc) CreateController(volumeName, controllerName string, replicas map[string]*types.ReplicaInfo) (*types.ControllerInfo, error) {
	data, err := d.prepareCreateController(volumeName, controllerName, replicas)
	if err != nil {
		return nil, errors.Wrapf(err, "Fail to create controller for %v", volumeName)
	}
	schedule := &types.ScheduleItem{
		Action: types.ScheduleActionCreateController,
		Instance: &types.ScheduleInstance{
			ID: controllerName,
		},
		Data: data,
	}
	instance, err := d.ProcessSchedule(schedule)
	if err != nil {
		return nil, errors.Wrapf(err, "Fail to create controller for %v", volumeName)
	}
	return &types.ControllerInfo{
		InstanceInfo: *instance,
	}, nil
}

func (d *dockerOrc) prepareCreateController(volumeName, controllerName string, replicas map[string]*types.ReplicaInfo) (*types.ScheduleData, error) {
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
	for _, replica := range replicas {
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

func (d *dockerOrc) createController(data *dockerScheduleData) (*types.InstanceInfo, error) {
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
	instance, err := d.startInstance(createBody.ID)
	if err != nil {
		logrus.Errorf("fail to start %v, cleaning up", data.InstanceName)
		d.removeInstance(createBody.ID)
		return nil, errors.Wrap(err, "fail to start controller container")
	}

	//FIXME different address format for controller
	instance.Address = "http://" + instance.Address + ":9501"

	url := instance.Address + "/v1"
	if err := util.WaitForAPI(url, WaitAPITimeout); err != nil {
		return nil, errors.Wrapf(err, "fail to wait for api endpoint at %v", url)
	}

	if err := util.WaitForDevice(d.getDeviceName(data.VolumeName), WaitDeviceTimeout); err != nil {
		return nil, errors.Wrap(err, "fail to wait for device")
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
		Instance: &types.ScheduleInstance{
			ID: replicaName,
		},
		Data: data,
	}
	instance, err := d.ProcessSchedule(schedule)
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

func (d *dockerOrc) createReplica(data *dockerScheduleData) (*types.InstanceInfo, error) {
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
		return nil, errors.Wrap(err, "fail to create replica container")
	}
	instance, err := d.startInstance(createBody.ID)
	if err != nil {
		logrus.Errorf("fail to start %v, cleaning up", data.InstanceName)
		d.removeInstance(createBody.ID)
		return nil, errors.Wrap(err, "fail to start replica container")
	}
	return instance, nil
}

func (d *dockerOrc) generateInstanceInfo(instanceID string) (*types.InstanceInfo, error) {
	inspectJSON, err := d.cli.ContainerInspect(context.Background(), instanceID)
	if err != nil {
		return nil, errors.Wrap(err, "fail to inspect replica container")
	}
	return &types.InstanceInfo{
		// It's weird that Docker put a forward slash to the container name
		// So it become "/replica-1"
		ID:      inspectJSON.ID,
		Name:    strings.TrimPrefix(inspectJSON.Name, "/"),
		HostID:  d.GetCurrentHostID(),
		Address: inspectJSON.NetworkSettings.IPAddress,
		Running: inspectJSON.State.Running,
	}, nil
}

func (d *dockerOrc) StartInstance(instance *types.InstanceInfo) error {
	data, err := d.prepareStartInstance(instance.ID)
	if err != nil {
		return errors.Wrapf(err, "Fail to start instance %v", instance.ID)
	}
	schedule := &types.ScheduleItem{
		Action: types.ScheduleActionStartInstance,
		Instance: &types.ScheduleInstance{
			ID:     instance.ID,
			HostID: instance.HostID,
		},
		Data: data,
	}
	if _, err := d.ProcessSchedule(schedule); err != nil {
		return errors.Wrapf(err, "Fail to start instance %v", instance.ID)
	}
	return nil
}

func (d *dockerOrc) prepareInstanceScheduleData(instanceID string) (*types.ScheduleData, error) {
	data := &dockerScheduleData{
		InstanceID: instanceID,
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

func (d *dockerOrc) prepareStartInstance(instanceID string) (*types.ScheduleData, error) {
	return d.prepareInstanceScheduleData(instanceID)
}

func (d *dockerOrc) startInstance(instanceID string) (*types.InstanceInfo, error) {
	if err := d.cli.ContainerStart(context.Background(),
		instanceID, dTypes.ContainerStartOptions{}); err != nil {
		return nil, errors.Wrapf(err, "fail to start instance '%v'", instanceID)
	}
	return d.generateInstanceInfo(instanceID)
}

func (d *dockerOrc) StopInstance(instance *types.InstanceInfo) error {
	data, err := d.prepareStopInstance(instance.ID)
	if err != nil {
		return errors.Wrapf(err, "Fail to stop instance %v", instance.ID)
	}
	schedule := &types.ScheduleItem{
		Action: types.ScheduleActionStopInstance,
		Instance: &types.ScheduleInstance{
			ID:     instance.ID,
			HostID: instance.HostID,
		},
		Data: data,
	}
	if _, err := d.ProcessSchedule(schedule); err != nil {
		return errors.Wrapf(err, "Fail to stop instance %v", instance.ID)
	}
	return nil
}

func (d *dockerOrc) prepareStopInstance(instanceID string) (*types.ScheduleData, error) {
	return d.prepareInstanceScheduleData(instanceID)
}

func (d *dockerOrc) stopInstance(instanceID string) (*types.InstanceInfo, error) {
	if err := d.cli.ContainerStop(context.Background(),
		instanceID, &ContainerStopTimeout); err != nil {
		return nil, errors.Wrapf(err, "fail to start instance '%v'", instanceID)
	}
	return d.generateInstanceInfo(instanceID)
}

func (d *dockerOrc) RemoveInstance(instance *types.InstanceInfo) error {
	data, err := d.prepareRemoveInstance(instance.ID)
	if err != nil {
		return errors.Wrapf(err, "Fail to remove instance %v", instance.ID)
	}
	schedule := &types.ScheduleItem{
		Action: types.ScheduleActionDeleteInstance,
		Instance: &types.ScheduleInstance{
			ID:     instance.ID,
			HostID: instance.HostID,
		},
		Data: data,
	}
	if _, err := d.ProcessSchedule(schedule); err != nil {
		return errors.Wrapf(err, "Fail to remove instance %v", instance.ID)
	}
	return nil
}

func (d *dockerOrc) prepareRemoveInstance(instanceID string) (*types.ScheduleData, error) {
	return d.prepareInstanceScheduleData(instanceID)
}

func (d *dockerOrc) removeInstance(instanceID string) (*types.InstanceInfo, error) {
	if err := d.cli.ContainerRemove(context.Background(), instanceID,
		dTypes.ContainerRemoveOptions{RemoveVolumes: true}); err != nil {
		if err != nil {
			return nil, errors.Wrapf(err, "Fail to remove instance %v", instanceID)
		}
	}
	return &types.InstanceInfo{
		ID: instanceID,
	}, nil
}

func (d *dockerOrc) GetSettings() (*types.SettingsInfo, error) {
	settings, err := d.getSettings()
	if err != nil {
		return nil, err
	}
	if settings == nil {
		return &types.SettingsInfo{
			BackupTarget:  "vfs:///var/lib/longhorn/backups/default",
			LonghornImage: d.LonghornImage,
		}, nil
	}
	return settings, nil
}

func (d *dockerOrc) SetSettings(settings *types.SettingsInfo) error {
	return d.setSettings(settings)
}
