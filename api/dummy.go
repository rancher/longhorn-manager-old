package api

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
)

type dummyVolumeManager struct{}

func DummyVolumeManager() types.VolumeManager {
	return &dummyVolumeManager{}
}

func (d *dummyVolumeManager) Create(volume *types.VolumeInfo) (*types.VolumeInfo, error) {
	return volume, nil
}

func (d *dummyVolumeManager) Delete(name string) error {
	return nil
}

func (d *dummyVolumeManager) Get(name string) (*types.VolumeInfo, error) {
	numberOfReplicas := 2
	replicas := map[string]*types.ReplicaInfo{}

	for i := 0; i < numberOfReplicas; i++ {
		index := fmt.Sprintf("rplc%v", i)
		replicas[index] = &types.ReplicaInfo{
			InstanceInfo: types.InstanceInfo{
				ID:      "inst" + index,
				HostID:  "host" + index,
				Address: util.ReplicaAddress("replica-"+index, name),
				Running: true,
			},
			Name: "replica-" + index,
			Mode: types.RW,
		}
	}

	state := types.Healthy

	return &types.VolumeInfo{
		Name:             name,
		Size:             107374182400,
		NumberOfReplicas: numberOfReplicas,
		Replicas:         replicas,
		State:            state,
		Controller: &types.ControllerInfo{
			InstanceInfo: types.InstanceInfo{
				ID:      "instidctrl",
				HostID:  "hostidctrl",
				Address: util.ControllerAddress(name),
				Running: true,
			},
		},
	}, nil
}

func (d *dummyVolumeManager) List() ([]*types.VolumeInfo, error) {
	vs := []*types.VolumeInfo{}
	for i := 0; i < 3; i++ {
		v, _ := d.Get(fmt.Sprintf("vol%v", i))
		vs = append(vs, v)
	}
	return vs, nil
}

func (d *dummyVolumeManager) Attach(name string) error {
	return nil
}

func (d *dummyVolumeManager) Detach(name string) error {
	return nil
}

func (d *dummyVolumeManager) CheckController(ctrl types.Controller, volume *types.VolumeInfo) error {
	return nil
}

func (d *dummyVolumeManager) Cleanup(volume *types.VolumeInfo) error {
	return nil
}

func (d *dummyVolumeManager) VolumeSnapshots(name string) (types.VolumeSnapshots, error) {
	return &dummySnapshots{}, nil
}

type dummyLocator struct {
	thisHostID string
}

func DummyServiceLocator(hostID string) types.ServiceLocator {
	logrus.Infof("New dummy SL: this hostID is '%s'", hostID)
	return &dummyLocator{hostID}
}

func (l *dummyLocator) GetAddress(q string) (string, error) {
	logrus.Infof("Dummy SL: resolving address for '%s', returning 'localhost' (we're a dummy)", q)
	return "localhost", nil
}

func (l *dummyLocator) IsLocal(q string) bool {
	logrus.Infof("Dummy SL: hostID='%s' is local?: %v", q, q == l.thisHostID)
	return q == l.thisHostID
}

type dummySnapshots struct{}

func (d *dummySnapshots) Create(name string) (string, error) {
	return "dummy", nil
}

func (d *dummySnapshots) List() ([]*types.SnapshotInfo, error) {
	return []*types.SnapshotInfo{}, nil
}

func (d *dummySnapshots) Get(name string) (*types.SnapshotInfo, error) {
	return &types.SnapshotInfo{Name: name}, nil
}

func (d *dummySnapshots) Delete(name string) error {
	return nil
}

func (d *dummySnapshots) Revert(name string) error {
	return nil
}
