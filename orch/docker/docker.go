package docker

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
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

	"github.com/rancher/longhorn-orc/orch"
	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
)

const (
	keyHosts   = "hosts"
	keyVolumes = "volumes"

	cfgDirectory = "/var/lib/rancher/longhorn/"
	hostUUIDFile = cfgDirectory + ".physical_host_uuid"
)

var (
	ContainerStopTimeout = 1 * time.Minute
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
	address string
	prefix  string
	image   string
}

func New(c *cli.Context) (types.Orchestrator, error) {
	servers := c.StringSlice("etcd-servers")
	if len(servers) == 0 {
		return nil, fmt.Errorf("Unspecified etcd servers")
	}
	address := c.String("host-address")
	prefix := c.String("etcd-prefix")
	image := c.String(orch.LonghornImageParam)
	return newDocker(&dockerOrcConfig{
		servers: servers,
		address: address,
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

	if err := docker.Register(cfg.address); err != nil {
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
	_, err := d.getVolume(volume.Name)
	if err == nil {
		return nil, errors.Errorf("volume %v already exists", volume.Name)
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

func (d *dockerOrc) MarkBadReplica(volumeName string, replica *types.ReplicaInfo) error {
	return nil
}

func (d *dockerOrc) CreateController(volumeName string, replicas map[string]*types.ReplicaInfo) (*types.ControllerInfo, error) {
	return nil, nil
}

func (d *dockerOrc) CreateReplica(volumeName string) (*types.ReplicaInfo, error) {
	volume, err := d.getVolume(volumeName)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create replica")
	}
	return d.createReplica("replica-"+util.UUID()[:8], volume)
}

func (d *dockerOrc) createReplica(replicaName string, volume *types.VolumeInfo) (*types.ReplicaInfo, error) {
	cmd := []string{
		"launch", "replica",
		"--listen", "0.0.0.0:9502",
		"--size", strconv.FormatInt(volume.Size, 10),
		"/volume",
	}
	createBody, err := d.cli.ContainerCreate(context.Background(), &dContainer.Config{
		ExposedPorts: dNat.PortSet{
			"9502-9504": struct{}{},
		},
		Image: volume.LonghornImage,
		Volumes: map[string]struct{}{
			"/volume": {},
		},
		Cmd: cmd,
	}, nil, nil, replicaName)
	if err != nil {
		return nil, errors.Wrap(err, "fail to create replica container")
	}
	if err := d.StartInstance(createBody.ID); err != nil {
		return nil, errors.Wrap(err, "fail to start replica container")
	}
	replicaJSON, err := d.cli.ContainerInspect(context.Background(), createBody.ID)
	if err != nil {
		return nil, errors.Wrap(err, "fail to inspect replica container")
	}
	return &types.ReplicaInfo{
		InstanceInfo: types.InstanceInfo{
			ID:      replicaJSON.ID,
			HostID:  d.GetCurrentHostID(),
			Address: replicaJSON.NetworkSettings.IPAddress,
			Running: replicaJSON.State.Running,
		},

		Name: replicaJSON.Name,
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

func (d *dockerOrc) GetSettings() *types.SettingsInfo {
	return &types.SettingsInfo{
		BackupTarget: "vfs:///var/lib/longhorn/backups/default",
		LonghornImage: d.LonghornImage,
	}
}

func (d *dockerOrc) SetSettings(*types.SettingsInfo) {
}
