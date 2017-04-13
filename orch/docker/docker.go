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
	dCli "github.com/docker/docker/client"

	"github.com/rancher/longhorn-manager/api"
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
	Servers       []string //etcd servers
	Prefix        string   //prefix in k/v store
	LonghornImage string
	Network       string

	currentHost *types.HostInfo

	kapi eCli.KeysAPI
	cli  *dCli.Client

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
	image := c.String(orch.LonghornImageParam)
	network := c.String("docker-network")
	return newDocker(&dockerOrcConfig{
		servers: servers,
		prefix:  prefix,
		image:   image,
		network: network,
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
		Network:       cfg.network,

		kapi: eCli.NewKeysAPI(etcdc),
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
	v, err := d.getVolume(volumeName)
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
	settings, err := d.getSettings()
	if err != nil {
		return nil, err
	}
	if settings == nil {
		return &types.SettingsInfo{
			BackupTarget:  "",
			LonghornImage: d.LonghornImage,
		}, nil
	}
	return settings, nil
}

func (d *dockerOrc) SetSettings(settings *types.SettingsInfo) error {
	return d.setSettings(settings)
}

func (d *dockerOrc) Scheduler() types.Scheduler {
	return d.scheduler
}
