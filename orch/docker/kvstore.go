package docker

import (
	"encoding/json"
	"path/filepath"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	eCli "github.com/coreos/etcd/client"

	"github.com/rancher/longhorn-orc/types"
)

func (d *dockerOrc) key(key string) string {
	// It's not file path, but we use it to deal with '/'
	return filepath.Join(d.Prefix, key)
}

func (d *dockerOrc) hostKey(id string) string {
	return filepath.Join(d.key(keyHosts), id)
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

func (d *dockerOrc) getHost(id string) (*types.HostInfo, error) {
	resp, err := d.kapi.Get(context.Background(), d.hostKey(id), nil)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get host")
	}
	return node2Host(resp.Node)
}

func (d *dockerOrc) listHosts() (map[string]*types.HostInfo, error) {
	resp, err := d.kapi.Get(context.Background(), d.key(keyHosts), nil)
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

func (d *dockerOrc) volumeKey(id string) string {
	return filepath.Join(d.key(keyVolumes), id)
}

func (d *dockerOrc) setVolume(volume *types.VolumeInfo) error {
	value, err := json.Marshal(volume)
	if err != nil {
		return err
	}
	if _, err := d.kapi.Set(context.Background(), d.volumeKey(volume.Name), string(value), nil); err != nil {
		return err
	}
	return nil
}

func (d *dockerOrc) getVolume(id string) (*types.VolumeInfo, error) {
	resp, err := d.kapi.Get(context.Background(), d.volumeKey(id), nil)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get volume")
	}
	return node2Volume(resp.Node)
}

func (d *dockerOrc) rmVolume(id string) error {
	_, err := d.kapi.Delete(context.Background(), d.volumeKey(id), &eCli.DeleteOptions{Recursive: true})
	if err != nil {
		return errors.Wrap(err, "unable to remove volume")
	}
	return nil
}

func (d *dockerOrc) listVolumes() (map[string]*types.VolumeInfo, error) {
	resp, err := d.kapi.Get(context.Background(), d.key(keyVolumes), nil)
	if err != nil {
		return nil, err
	}

	if !resp.Node.Dir {
		return nil, errors.Errorf("Invalid node %v is not a directory",
			resp.Node.Key)
	}

	volumes := make(map[string]*types.VolumeInfo)
	for _, node := range resp.Node.Nodes {
		volume, err := node2Volume(node)
		if err != nil {
			return nil, errors.Wrapf(err, "Invalid node %v:%v, %v",
				node.Key, node.Value, err)
		}
		volumes[volume.Name] = volume
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
