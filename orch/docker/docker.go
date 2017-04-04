package docker

import (
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
	address := ips[0] + ":" + strconv.Itoa(api.Port)

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

func (d *dockerOrc) CreateController(volumeName string, replicas map[string]*types.ReplicaInfo) (*types.ControllerInfo, error) {
	volume, err := d.getVolume(volumeName)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create replica")
	}
	return d.createController(volume, replicas)
}

func (d *dockerOrc) createController(volume *types.VolumeInfo, replicas map[string]*types.ReplicaInfo) (*types.ControllerInfo, error) {
	controllerName := volume.Name + "-controller"
	cmd := []string{
		"launch", "controller",
		"--listen", "0.0.0.0:9501",
		"--frontend", "tgt",
	}
	for _, replica := range replicas {
		cmd = append(cmd, "--replica", "tcp://"+replica.Address+":9502")
	}
	cmd = append(cmd, volume.Name)

	createBody, err := d.cli.ContainerCreate(context.Background(),
		&dContainer.Config{
			Image: volume.LonghornImage,
			Cmd:   cmd,
		},
		&dContainer.HostConfig{
			Binds: []string{
				"/dev:/host/dev",
				"/proc:/host/proc",
			},
			Privileged: true,
		}, nil, controllerName)
	if err != nil {
		return nil, errors.Wrap(err, "fail to create controller container")
	}
	if err := d.StartInstance(createBody.ID); err != nil {
		logrus.Errorf("fail to start %v, cleaning up", controllerName)
		d.RemoveInstance(createBody.ID)
		return nil, errors.Wrap(err, "fail to start controller container")
	}
	inspectJSON, err := d.cli.ContainerInspect(context.Background(), createBody.ID)
	if err != nil {
		return nil, errors.Wrap(err, "fail to inspect controller container")
	}

	address := "http://" + inspectJSON.NetworkSettings.IPAddress + ":9501"
	url := address + "/v1"
	if err := util.WaitForAPI(url, WaitAPITimeout); err != nil {
		return nil, errors.Wrapf(err, "fail to wait for api endpoint at %v", url)
	}

	if err := util.WaitForDevice(d.getDeviceName(volume.Name), WaitDeviceTimeout); err != nil {
		return nil, errors.Wrap(err, "fail to wait for device")
	}

	return &types.ControllerInfo{
		InstanceInfo: types.InstanceInfo{
			ID:      inspectJSON.ID,
			HostID:  d.GetCurrentHostID(),
			Address: address,
			Running: inspectJSON.State.Running,
		},
	}, nil
}

func (d *dockerOrc) getDeviceName(volumeName string) string {
	return filepath.Join("/dev/longhorn/", volumeName)
}

func (d *dockerOrc) CreateReplica(volumeName string) (*types.ReplicaInfo, error) {
	volume, err := d.getVolume(volumeName)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create replica")
	}
	return d.createReplica(volume, volumeName+"-replica-"+util.RandomID())
}

func (d *dockerOrc) createReplica(volume *types.VolumeInfo, replicaName string) (*types.ReplicaInfo, error) {
	cmd := []string{
		"launch", "replica",
		"--listen", "0.0.0.0:9502",
		"--size", strconv.FormatInt(volume.Size, 10),
		"/volume",
	}
	createBody, err := d.cli.ContainerCreate(context.Background(),
		&dContainer.Config{
			ExposedPorts: dNat.PortSet{
				"9502-9504": struct{}{},
			},
			Image: volume.LonghornImage,
			Volumes: map[string]struct{}{
				"/volume": {},
			},
			Cmd: cmd,
		},
		&dContainer.HostConfig{
			Privileged: true,
		}, nil, replicaName)
	if err != nil {
		return nil, errors.Wrap(err, "fail to create replica container")
	}
	if err := d.StartInstance(createBody.ID); err != nil {
		logrus.Errorf("fail to start %v, cleaning up", replicaName)
		d.RemoveInstance(createBody.ID)
		return nil, errors.Wrap(err, "fail to start replica container")
	}
	inspectJSON, err := d.cli.ContainerInspect(context.Background(), createBody.ID)
	if err != nil {
		return nil, errors.Wrap(err, "fail to inspect replica container")
	}
	return &types.ReplicaInfo{
		InstanceInfo: types.InstanceInfo{
			ID:      inspectJSON.ID,
			HostID:  d.GetCurrentHostID(),
			Address: inspectJSON.NetworkSettings.IPAddress,
			Running: inspectJSON.State.Running,
		},

		// It's weird that Docker put a forward slash to the container name
		// So it become "/replica-test-1"
		Name: strings.TrimPrefix(inspectJSON.Name, "/"),
		//TODO: Mode
	}, nil
}

func (d *dockerOrc) StartInstance(instanceID string) error {
	return d.cli.ContainerStart(context.Background(), instanceID, dTypes.ContainerStartOptions{})
}

func (d *dockerOrc) StopInstance(instanceID string) error {
	return d.cli.ContainerStop(context.Background(), instanceID, &ContainerStopTimeout)
}

func (d *dockerOrc) RemoveInstance(instanceID string) error {
	return d.cli.ContainerRemove(context.Background(), instanceID, dTypes.ContainerRemoveOptions{RemoveVolumes: true})
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
