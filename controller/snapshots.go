package controller

import (
	"encoding/json"
	"os/exec"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

const (
	VolumeHeadName = "volume-head"
	purgeTimeout   = 15 * time.Minute
)

func (c *controller) SnapshotOps() types.SnapshotOps {
	return c
}

func (c *controller) Create(name string, labels map[string]string) (string, error) {
	args := []string{"--url", c.url, "snapshot", "create"}
	for k, v := range labels {
		args = append(args, "--label", k+"="+v)
	}
	args = append(args, name)

	output, err := util.Execute("longhorn", args...)
	if err != nil {
		return "", errors.Wrapf(err, "error creating snapshot '%s'", name)
	}
	return strings.TrimSpace(output), nil
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
	if _, err := util.Execute("longhorn", "--url", c.url,
		"snapshot", "rm", name); err != nil {
		return errors.Wrapf(err, "error deleting snapshot '%s'", name)
	}
	return nil
}

func (c *controller) Revert(name string) error {
	if _, err := util.Execute("longhorn", "--url", c.url,
		"snapshot", "revert", name); err != nil {
		return errors.Wrapf(err, "error reverting to snapshot '%s'", name)
	}
	return nil
}

func (c *controller) Purge() error {
	logrus.Debugf("Snapshot purge called, volume '%s', purgeQueue '%v'", c.name, c.purgeQueue)

	select {
	case c.purgeQueue <- struct{}{}:
		defer func() { <-c.purgeQueue }()
	default:
		logrus.Debugf("Skipping snapshot purge: another one is pending, volume '%s'", c.name)
		return nil
	}

	c.Lock()
	defer c.Unlock()
	if _, err := util.ExecuteWithTimeout(purgeTimeout, "longhorn", "--url", c.url,
		"snapshot", "purge"); err != nil {
		return errors.Wrapf(err, "error purging snapshots")
	}
	return nil
}
