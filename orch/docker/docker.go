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

	dTypes "github.com/docker/docker/api/types"
	dCli "github.com/docker/docker/client"

	"github.com/rancher/longhorn-manager/api"
	"github.com/rancher/longhorn-manager/kvstore"
	"github.com/rancher/longhorn-manager/orch"
	"github.com/rancher/longhorn-manager/scheduler"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

const (
	cfgDirectory = "/var/lib/rancher/longhorn/"
	hostUUIDFile = cfgDirectory + ".physical_host_uuid"
)

type dockerOrc struct {
	EngineImage string
	Network     string
	IP          string

	currentHost *types.HostInfo

	kv  *kvstore.KVStore
	cli *dCli.Client

	scheduler types.Scheduler
}

type dockerOrcConfig struct {
	servers []string
	prefix  string
	image   string
	network string
}

func New(c *cli.Context) (types.Orchestrator, error) {
	servers := c.StringSlice("etcd-servers")
	if len(servers) == 0 {
		return nil, fmt.Errorf("Unspecified etcd servers")
	}
	prefix := c.String("etcd-prefix")
	image := c.String(orch.EngineImageParam)
	network := c.String("docker-network")
	return newDocker(&dockerOrcConfig{
		servers: servers,
		prefix:  prefix,
		image:   image,
		network: network,
	})
}

func newDocker(cfg *dockerOrcConfig) (types.Orchestrator, error) {
	kvStore, err := kvstore.NewKVStore(cfg.servers, cfg.prefix)
	if err != nil {
		return nil, err
	}

	docker := &dockerOrc{
		EngineImage: cfg.image,
		kv:          kvStore,
	}
	docker.scheduler = scheduler.NewOrcScheduler(docker)

	//Set Docker API to compatible with 1.12
	os.Setenv("DOCKER_API_VERSION", "1.24")
	docker.cli, err = dCli.NewEnvClient()
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to docker")
	}

	if _, err := docker.cli.ContainerList(context.Background(), dTypes.ContainerListOptions{}); err != nil {
		return nil, errors.Wrap(err, "cannot pass test to get container list")
	}

	if err = docker.updateNetwork(cfg.network); err != nil {
		return nil, errors.Wrapf(err, "fail to detect dedicated container network: %v", cfg.network)
	}

	logrus.Infof("Detected network is %s, IP is %s", docker.Network, docker.IP)

	address := docker.IP + ":" + strconv.Itoa(api.DefaultPort)
	logrus.Info("Local address is: ", address)

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

func (d *dockerOrc) updateNetwork(userSpecifiedNetwork string) error {
	containerID := os.Getenv("HOSTNAME")

	inspectJSON, err := d.cli.ContainerInspect(context.Background(), containerID)
	if err != nil {
		return errors.Errorf("cannot find manager container, may not be running inside container")
	}
	networks := inspectJSON.NetworkSettings.Networks
	if len(networks) == 0 {
		return errors.Errorf("cannot find manager container's network")
	}
	if userSpecifiedNetwork != "" {
		net := networks[userSpecifiedNetwork]
		if net == nil {
			return errors.Errorf("user specified network %v doesn't exist", userSpecifiedNetwork)
		}
		d.Network = userSpecifiedNetwork
		d.IP = net.IPAddress
		return nil
	}
	if len(networks) > 1 {
		return errors.Errorf("found multiple networks for container %v, "+
			"unable to decide which one to use, "+
			"use --docker-network option to specify: %+v", containerID, networks)
	}
	// only one entry here
	for k, v := range networks {
		d.Network = k
		d.IP = v.IPAddress
	}
	return nil
}

func (d *dockerOrc) Register(address string) error {
	currentHost, err := getCurrentHost(address)
	if err != nil {
		return err
	}

	if err := d.kv.SetHost(currentHost); err != nil {
		return err
	}
	d.currentHost = currentHost
	return nil
}

func (d *dockerOrc) GetHost(id string) (*types.HostInfo, error) {
	return d.kv.GetHost(id)
}

func (d *dockerOrc) ListHosts() (map[string]*types.HostInfo, error) {
	return d.kv.ListHosts()
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
	v, err := d.kv.GetVolumeBase(volume.Name)
	if err == nil && v != nil {
		return nil, errors.Errorf("volume %v already exists %+v", volume.Name, v)
	}
	if err := d.kv.SetVolumeBase(volume); err != nil {
		return nil, errors.Wrap(err, "fail to create new volume metadata")
	}
	return volume, nil
}

func (d *dockerOrc) DeleteVolume(volumeName string) error {
	return d.kv.DeleteVolume(volumeName)
}

func (d *dockerOrc) GetVolume(volumeName string) (*types.VolumeInfo, error) {
	return d.kv.GetVolume(volumeName)
}

func (d *dockerOrc) UpdateVolume(volume *types.VolumeInfo) error {
	v, err := d.kv.GetVolumeBase(volume.Name)
	if err != nil {
		return errors.Errorf("cannot update volume %v because it doesn't exists %+v", volume.Name, v)
	}
	return d.kv.SetVolumeBase(volume)
}

func (d *dockerOrc) ListVolumes() ([]*types.VolumeInfo, error) {
	return d.kv.ListVolumes()
}

func (d *dockerOrc) MarkBadReplica(volumeName string, replica *types.ReplicaInfo) error {
	v, err := d.kv.GetVolume(volumeName)
	if err != nil {
		return errors.Wrap(err, "fail to mark bad replica, cannot get volume")
	}
	for k, r := range v.Replicas {
		if r.Name == replica.Name {
			r.BadTimestamp = time.Now().UTC()
			v.Replicas[k] = r
			break
		}
	}
	if err := d.UpdateVolume(v); err != nil {
		return errors.Wrap(err, "fail to mark bad replica, cannot update volume")
	}
	return nil
}

func (d *dockerOrc) GetSettings() (*types.SettingsInfo, error) {
	settings, err := d.kv.GetSettings()
	if err != nil {
		return nil, err
	}
	if settings == nil {
		return &types.SettingsInfo{
			BackupTarget: "",
			EngineImage:  d.EngineImage,
		}, nil
	}
	return settings, nil
}

func (d *dockerOrc) SetSettings(settings *types.SettingsInfo) error {
	return d.kv.SetSettings(settings)
}

func (d *dockerOrc) Scheduler() types.Scheduler {
	return d.scheduler
}
