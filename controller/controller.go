package controller

import (
	"encoding/json"
	"os/exec"
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
)

func init() {
	go holdControllers()
}

var reqCh = make(chan *req)

type req struct {
	volume *types.VolumeInfo
	result chan *controller
}

func ctrlReq(volume *types.VolumeInfo) *req {
	return &req{volume: volume, result: make(chan *controller)}
}

func holdControllers() {
	cs := map[string]*controller{}

	for r := range reqCh {
		if r.volume.Controller == nil || !r.volume.Controller.Running {
			delete(cs, r.volume.Name)
			close(r.result)
			continue
		}
		c := cs[r.volume.Name]
		if c == nil || c.url != r.volume.Controller.Address {
			c = &controller{name: r.volume.Name, url: r.volume.Controller.Address}
			cs[r.volume.Name] = c
		}
		r.result <- c
	}
}

type controller struct {
	sync.Mutex

	name string
	url  string

	backupStatus     *types.BackupStatusInfo
	backupStatusLock sync.Mutex
}

type volumeInfo struct {
	Name         string `json:"name"`
	ReplicaCount int    `json:"replicaCount"`
	Endpoint     string `json:"endpoint"`
}

func Get(volume *types.VolumeInfo) types.Controller {
	if volume == nil || volume.Controller == nil || !volume.Controller.Running {
		return nil
	}
	req := ctrlReq(volume)
	reqCh <- req
	return <-req.result
}

func Cleanup(volume *types.VolumeInfo) {
	volume = util.CopyVolumeProperties(volume)
	volume.Controller = nil
	reqCh <- ctrlReq(volume)
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
	if _, err := util.Execute("longhorn", "--url", c.url, "add", replica.Address); err != nil {
		return errors.Wrapf(err, "failed to add replica address='%s' to controller '%s'", replica.Address, c.name)
	}
	return nil
}

func (c *controller) RemoveReplica(replica *types.ReplicaInfo) error {
	if _, err := util.Execute("longhorn", "--url", c.url, "rm", replica.Address); err != nil {
		return errors.Wrapf(err, "failed to rm replica address='%s' from controller '%s'", replica.Address, c.name)
	}
	return nil
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
	output, err := util.Execute("longhorn", "--url", c.url, "info")
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get volume info")
	}

	info := &volumeInfo{}
	if err := json.Unmarshal([]byte(output), info); err != nil {
		return nil, errors.Wrapf(err, "cannot decode volume info: %v", output)
	}
	return info, nil
}
