package controller

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
)

type controller struct {
	name string
	url  string
}

type volumeInfo struct {
	Name         string `json:"name"`
	ReplicaCount int    `json:"replicaCount"`
	Endpoint     string `json:"endpoint"`
}

func New(volume *types.VolumeInfo) types.Controller {
	if volume == nil || volume.Controller == nil || !volume.Controller.Running {
		return nil
	}
	url := volume.Controller.Address
	return &controller{
		name: volume.Name,
		url:  url,
	}
}

func (c *controller) Name() string {
	return c.name
}

var modes = map[string]types.ReplicaMode{
	"RW":  types.ReplicaModeRW,
	"WO":  types.ReplicaModeWO,
	"ERR": types.ReplicaModeERR,
}

func parseReplica(s string) (*types.ReplicaInfo, error) {
	fields := strings.Fields(s)
	if len(fields) < 2 {
		return nil, errors.Errorf("cannot parse line `%s`", s)
	}
	mode, ok := modes[fields[1]]
	if !ok {
		mode = types.ReplicaModeERR
	}
	return &types.ReplicaInfo{
		InstanceInfo: types.InstanceInfo{
			Address: fields[0],
		},
		Mode: mode,
	}, nil
}

func trimChain(s string) string {
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		s = strings.TrimPrefix(s, "[")
		s = strings.TrimSuffix(s, "]")
		fields := strings.Fields(s)
		if len(fields) > 0 && strings.HasPrefix(fields[0], "volume-head-") {
			s = s[len(fields[0]):]
			s = strings.TrimSpace(s)
		}
	}
	return s
}

func (c *controller) GetReplicaStates() ([]*types.ReplicaInfo, error) {
	replicas := []*types.ReplicaInfo{}
	cancel := make(chan interface{})
	defer close(cancel)
	lineCh, cliErrCh := util.CmdOutLines(exec.Command("longhorn", "--url", c.url, "ls"), cancel)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	parsingErrCh := make(chan error)
	go func() {
		defer wg.Done()
		defer close(parsingErrCh)
		for s := range lineCh {
			if strings.HasPrefix(s, "ADDRESS") {
				continue
			}
			replica, err := parseReplica(s)
			if err != nil {
				parsingErrCh <- errors.Wrapf(err, "error parsing replica status from `%s`", s)
				break
			}
			replicas = append(replicas, replica)
		}
	}()
	for err := range parsingErrCh {
		return nil, err
	}
	for err := range cliErrCh {
		return nil, err
	}

	wg.Wait()
	return replicas, nil
}

func (c *controller) AddReplica(replica *types.ReplicaInfo) error {
	err := exec.Command("longhorn", "--url", c.url, "add", replica.Address).Run()
	return errors.Wrapf(err, "failed to add replica address='%s' to controller '%s'", replica.Address, c.name)
}

func (c *controller) RemoveReplica(replica *types.ReplicaInfo) error {
	err := exec.Command("longhorn", "--url", c.url, "rm", replica.Address).Run()
	return errors.Wrapf(err, "failed to rm replica address='%s' from controller '%s'", replica.Address, c.name)
}

func (c *controller) Endpoint() string {
	info, err := c.info()
	if err != nil {
		logrus.Warn("Fail to get frontend info: ", err)
		return ""
	}

	return info.Endpoint
}

func (c *controller) info() (*volumeInfo, error) {
	var stdout, stderr bytes.Buffer

	cmd := exec.Command("longhorn", "--url", c.url, "info")
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get volume info: %v", stderr.String())
	}

	info := &volumeInfo{}
	if err := json.Unmarshal(stdout.Bytes(), info); err != nil {
		return nil, errors.Wrapf(err, "cannot decode volume info: %v", stdout.String())
	}
	return info, nil
}
