package controller

import (
	"encoding/json"
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-orc/types"
	"os/exec"
	"strings"
)

const VolumeHeadName = "volume-head"

func (c *controller) Snapshots() types.VolumeSnapshots {
	return c
}

func (c *controller) Create(name string) (string, error) {
	cmd := exec.Command("longhorn", "--url", c.url, "snapshot", "create", name)
	bytes, err := cmd.Output()
	if err != nil {
		return "", errors.Wrapf(err, "error creating snapshot '%s'", name)
	}
	return strings.TrimSpace(string(bytes)), nil
}

func (c *controller) list() (map[string]*types.SnapshotInfo, error) {
	cmd := exec.Command("longhorn", "--url", c.url, "snapshot", "info")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrapf(err, "error getting stdout from cmd '%v'", cmd)
	}
	if err := cmd.Start(); err != nil {
		return nil, errors.Wrapf(err, "error starting cmd '%v'", cmd)
	}
	defer func() {
		if err := cmd.Wait(); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error waiting for cmd '%v'", cmd))
		}
	}()
	data := map[string]*types.SnapshotInfo{}
	if err := json.NewDecoder(stdout).Decode(&data); err != nil {
		return nil, errors.Wrapf(err, "error parsing data from cmd '%v'", cmd)
	}
	delete(data, VolumeHeadName)
	return data, nil
}

func (c *controller) List() ([]*types.SnapshotInfo, error) {
	data, err := c.list()
	if err != nil {
		return nil, err
	}
	ss := []*types.SnapshotInfo{}
	for _, s := range data {
		ss = append(ss, s)
	}
	return ss, nil
}

func (c *controller) Get(name string) (*types.SnapshotInfo, error) {
	data, err := c.list()
	if err != nil {
		return nil, err
	}
	return data[name], nil
}

func (c *controller) Delete(name string) error {
	cmd := exec.Command("longhorn", "--url", c.url, "snapshot", "rm", name)
	err := cmd.Run()
	if err != nil {
		return errors.Wrapf(err, "error deleting snapshot '%s'", name)
	}
	return nil
}

func (c *controller) Revert(name string) error {
	cmd := exec.Command("longhorn", "--url", c.url, "snapshot", "revert", name)
	err := cmd.Run()
	if err != nil {
		return errors.Wrapf(err, "error reverting to snapshot '%s'", name)
	}
	return nil
}
