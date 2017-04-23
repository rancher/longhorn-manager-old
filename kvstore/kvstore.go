package kvstore

import (
	"encoding/json"
	"path/filepath"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	eCli "github.com/coreos/etcd/client"

	"github.com/rancher/longhorn-manager/types"
)

type KVStore struct {
	Servers []string
	Prefix  string

	kapi eCli.KeysAPI
}

const (
	keyHosts    = "hosts"
	keyVolumes  = "volumes"
	keySettings = "settings"
)

func NewKVStore(servers []string, prefix string) (*KVStore, error) {
	eCfg := eCli.Config{
		Endpoints:               servers,
		Transport:               eCli.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}

	etcdc, err := eCli.New(eCfg)
	if err != nil {
		return nil, err
	}
	kvStore := &KVStore{
		Servers: servers,
		Prefix:  prefix,

		kapi: eCli.NewKeysAPI(etcdc),
	}
	return kvStore, nil
}

func (s *KVStore) key(key string) string {
	// It's not file path, but we use it to deal with '/'
	return filepath.Join(s.Prefix, key)
}

func (s *KVStore) hostKey(id string) string {
	return filepath.Join(s.key(keyHosts), id)
}

func (s *KVStore) SetHost(host *types.HostInfo) error {
	value, err := json.Marshal(host)
	if err != nil {
		return err
	}
	if _, err := s.kvSet(s.hostKey(host.UUID), string(value), nil); err != nil {
		return err
	}
	logrus.Infof("Add host %v name %v longhorn-manager address %v", host.UUID, host.Name, host.Address)
	return nil
}

func (s *KVStore) GetHost(id string) (*types.HostInfo, error) {
	resp, err := s.kvGet(s.hostKey(id), nil)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get host")
	}
	return node2Host(resp.Node)
}

func (s *KVStore) ListHosts() (map[string]*types.HostInfo, error) {
	resp, err := s.kvGet(s.key(keyHosts), nil)
	if err != nil {
		return nil, err
	}

	if !resp.Node.Dir {
		return nil, errors.Errorf("Invalid node %v is not a directory",
			resp.Node.Key)
	}

	hosts := make(map[string]*types.HostInfo)
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

func node2Host(node *eCli.Node) (*types.HostInfo, error) {
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

func (s *KVStore) volumeKey(id string) string {
	return filepath.Join(s.key(keyVolumes), id)
}

func (s *KVStore) SetVolume(volume *types.VolumeInfo) error {
	value, err := json.Marshal(volume)
	if err != nil {
		return err
	}
	if _, err := s.kvSet(s.volumeKey(volume.Name), string(value), nil); err != nil {
		return err
	}
	return nil
}

func (s *KVStore) GetVolume(id string) (*types.VolumeInfo, error) {
	resp, err := s.kvGet(s.volumeKey(id), nil)
	if err != nil {
		if eCli.IsKeyNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "unable to get volume")
	}
	return node2Volume(resp.Node)
}

func (s *KVStore) DeleteVolume(id string) error {
	_, err := s.kvDelete(s.volumeKey(id), &eCli.DeleteOptions{Recursive: true})
	if err != nil {
		return errors.Wrap(err, "unable to remove volume")
	}
	return nil
}

func (s *KVStore) ListVolumes() ([]*types.VolumeInfo, error) {
	resp, err := s.kvGet(s.key(keyVolumes), nil)
	if err != nil {
		if eCli.IsKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	if !resp.Node.Dir {
		return nil, errors.Errorf("Invalid node %v is not a directory",
			resp.Node.Key)
	}

	volumes := []*types.VolumeInfo{}
	for _, node := range resp.Node.Nodes {
		volume, err := node2Volume(node)
		if err != nil {
			return nil, errors.Wrapf(err, "Invalid node %v:%v, %v",
				node.Key, node.Value, err)
		}
		volumes = append(volumes, volume)
	}
	return volumes, nil
}

func node2Volume(node *eCli.Node) (*types.VolumeInfo, error) {
	volume := &types.VolumeInfo{}
	if node.Dir {
		return nil, errors.Errorf("Invalid node %v is a directory",
			node.Key)
	}
	if err := json.Unmarshal([]byte(node.Value), volume); err != nil {
		return nil, errors.Wrap(err, "fail to unmarshall json for volume")
	}
	return volume, nil
}

func (s *KVStore) settingsKey() string {
	return s.key(keySettings)
}

func (s *KVStore) SetSettings(settings *types.SettingsInfo) error {
	value, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	if _, err := s.kvSet(s.settingsKey(), string(value), nil); err != nil {
		return err
	}
	return nil
}

func (s *KVStore) GetSettings() (*types.SettingsInfo, error) {
	resp, err := s.kvGet(s.settingsKey(), nil)
	if err != nil {
		if eCli.IsKeyNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "unable to get settings")
	}

	settings := &types.SettingsInfo{}
	node := resp.Node
	if node.Dir {
		return nil, errors.Errorf("Invalid node %v is a directory",
			node.Key)
	}
	if err := json.Unmarshal([]byte(node.Value), settings); err != nil {
		return nil, errors.Wrap(err, "fail to unmarshall json for settings")
	}
	return settings, nil
}

func (s *KVStore) kvSet(key, value string, opts *eCli.SetOptions) (*eCli.Response, error) {
	return s.kapi.Set(context.Background(), key, value, opts)
}

func (s *KVStore) kvGet(key string, opts *eCli.GetOptions) (*eCli.Response, error) {
	return s.kapi.Get(context.Background(), key, opts)
}

func (s *KVStore) kvDelete(key string, opts *eCli.DeleteOptions) (*eCli.Response, error) {
	return s.kapi.Delete(context.Background(), key, opts)
}
