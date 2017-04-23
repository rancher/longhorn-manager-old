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
	if err := s.kvSet(s.hostKey(host.UUID), host); err != nil {
		return err
	}
	logrus.Infof("Add host %v name %v longhorn-manager address %v", host.UUID, host.Name, host.Address)
	return nil
}

func (s *KVStore) GetHost(id string) (*types.HostInfo, error) {
	host, err := s.getHostByKey(s.hostKey(id))
	if err != nil {
		return nil, errors.Wrap(err, "unable to get host")
	}
	return host, nil
}

func (s *KVStore) getHostByKey(key string) (*types.HostInfo, error) {
	host := types.HostInfo{}
	if err := s.kvGet(key, &host); err != nil {
		if s.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	return &host, nil
}

func (s *KVStore) ListHosts() (map[string]*types.HostInfo, error) {
	hostKeys, err := s.kvListKeys(s.key(keyHosts))
	if err != nil {
		return nil, err
	}

	hosts := make(map[string]*types.HostInfo)
	for _, key := range hostKeys {
		host, err := s.getHostByKey(key)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid key %v", key)
		}
		if host != nil {
			hosts[host.UUID] = host
		}
	}
	return hosts, nil
}

func (s *KVStore) settingsKey() string {
	return s.key(keySettings)
}

func (s *KVStore) SetSettings(settings *types.SettingsInfo) error {
	if err := s.kvSet(s.settingsKey(), settings); err != nil {
		return err
	}
	return nil
}

func (s *KVStore) GetSettings() (*types.SettingsInfo, error) {
	settings := &types.SettingsInfo{}
	if err := s.kvGet(s.settingsKey(), &settings); err != nil {
		if s.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "unable to get settings")
	}

	return settings, nil
}

func (s *KVStore) kvSet(key string, obj interface{}) error {
	value, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	if _, err := s.kapi.Set(context.Background(), key, string(value), nil); err != nil {
		return err
	}
	return nil
}

func (s *KVStore) IsNotFoundError(err error) bool {
	return eCli.IsKeyNotFound(err)
}

func (s *KVStore) kvGet(key string, obj interface{}) error {
	resp, err := s.kapi.Get(context.Background(), key, nil)
	if err != nil {
		return err
	}
	node := resp.Node
	if node.Dir {
		return errors.Errorf("invalid node %v is a directory",
			node.Key)
	}
	if err := json.Unmarshal([]byte(node.Value), obj); err != nil {
		return errors.Wrap(err, "fail to unmarshal json")
	}
	return nil
}

func (s *KVStore) kvListKeys(key string) ([]string, error) {
	resp, err := s.kapi.Get(context.Background(), key, nil)
	if err != nil {
		if eCli.IsKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if !resp.Node.Dir {
		return nil, errors.Errorf("invalid node %v is not a directory",
			resp.Node.Key)
	}

	ret := []string{}
	for _, node := range resp.Node.Nodes {
		ret = append(ret, node.Key)
	}
	return ret, nil
}

func (s *KVStore) kvDelete(key string, recursive bool) error {
	_, err := s.kapi.Delete(context.Background(), key, &eCli.DeleteOptions{
		Recursive: recursive,
	})
	if err != nil {
		if eCli.IsKeyNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// kuNuclear is test only function, which will wipe all longhorn entries
func (s *KVStore) kvNuclear(nuclearCode string) error {
	if nuclearCode != "nuke key value store" {
		return errors.Errorf("invalid nuclear code!")
	}
	if err := s.kvDelete(s.key(""), true); err != nil {
		return err
	}
	return nil
}
