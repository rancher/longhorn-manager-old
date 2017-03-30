package docker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/coreos/etcd/client"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
	"golang.org/x/net/context"

	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
)

const (
	keyHosts   = "hosts"
	keyVolumes = "volumes"

	cfgDirectory = "/var/lib/rancher/longhorn/"
	hostUUIDFile = cfgDirectory + ".physical_host_uuid"
)

type dockerOrc struct {
	Servers []string //etcd servers
	Prefix  string   //prefix in k/v store

	kapi client.KeysAPI
}

func New(c *cli.Context) (types.Orchestrator, error) {
	servers := c.StringSlice("etcd-servers")
	if len(servers) == 0 {
		return nil, fmt.Errorf("Unspecified etcd servers")
	}
	address := c.String("host-address")

	cfg := client.Config{
		Endpoints:               servers,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}

	etcdc, err := client.New(cfg)
	if err != nil {
		return nil, err
	}

	docker := &dockerOrc{
		Servers: servers,
		Prefix:  c.String("etcd-prefix"),
		kapi:    client.NewKeysAPI(etcdc),
	}

	if err := docker.Register(address); err != nil {
		return nil, err
	}
	return docker, nil
}

func (d *dockerOrc) key(key string) string {
	// It's not file path, but we use it to deal with '/'
	return filepath.Join(d.Prefix, key)
}

func (d *dockerOrc) hostKey(id string) string {
	return filepath.Join(d.key(keyHosts), id)
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

	return d.setHost(currentHost)
}

func (d *dockerOrc) setHost(host *types.HostInfo) error {
	value, err := json.Marshal(host)
	if err != nil {
		return err
	}
	if _, err := d.kapi.Set(context.Background(), d.hostKey(host.UUID), string(value), nil); err != nil {
		return err
	}
	logrus.Infof("Add host %v name %v longhorn-orc address %v", host.UUID, host.Name, host.Address)
	return nil
}

func (d *dockerOrc) ListHosts() (map[string]*types.HostInfo, error) {
	resp, err := d.kapi.Get(context.Background(), d.key(keyHosts), nil)
	if err != nil {
		return nil, err
	}
	hosts := make(map[string]*types.HostInfo)

	if !resp.Node.Dir {
		return nil, errors.Errorf("Invalid node %v is not a directory",
			resp.Node.Key)
	}

	for _, node := range resp.Node.Nodes {
		host, err := node2Host(node)
		if err != nil {
			return nil, errors.Wrapf(err, "Invalid node %v:%v, %v",
				node.Key, node.Value, err)
		}
		hosts[host.UUID] = host
	}
	return hosts, nil
}

func (d *dockerOrc) GetHost(id string) (*types.HostInfo, error) {
	resp, err := d.kapi.Get(context.Background(), d.hostKey(id), nil)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get host")
	}
	return node2Host(resp.Node)
}

func node2Host(node *client.Node) (*types.HostInfo, error) {
	host := &types.HostInfo{}
	if node.Dir {
		return nil, errors.Errorf("Invalid node %v is a directory",
			node.Key)
	}
	if err := json.Unmarshal([]byte(node.Value), host); err != nil {
		return nil, errors.Wrap(err, "fail to unmarshall json for host")
	}
	return host, nil
}

func (d *dockerOrc) CreateVolume(volume *types.VolumeInfo) (*types.VolumeInfo, error) {
	return nil, nil
}

func (d *dockerOrc) DeleteVolume(volumeName string) error {
	return nil
}

func (d *dockerOrc) GetVolume(volumeName string) (*types.VolumeInfo, error) {
	return nil, nil
}

func (d *dockerOrc) MarkBadReplica(volumeName string, replica *types.ReplicaInfo) error {
	return nil
}

func (d *dockerOrc) CreateController(volumeName string, replicas map[string]*types.ReplicaInfo) (*types.ControllerInfo, error) {
	return nil, nil
}

func (d *dockerOrc) CreateReplica(volumeName string) (*types.ReplicaInfo, error) {
	return nil, nil
}

func (d *dockerOrc) StartInstance(instanceID string) error {
	return nil
}

func (d *dockerOrc) StopInstance(instanceID string) error {
	return nil
}

func (d *dockerOrc) RemoveInstance(instanceID string) error {
	return nil
}

func (d *dockerOrc) GetCurrentHostID() string {
	return ""
}

func (d *dockerOrc) GetAddress(hostID string) (string, error) {
	return "", nil
}
