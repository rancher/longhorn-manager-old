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
			Mode: types.ReplicaModeRW,
		}
	}

	state := types.VolumeStateHealthy

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

func (d *dummyVolumeManager) Start() error {
	return nil
}

func (d *dummyVolumeManager) Attach(name string) error {
	return nil
}

func (d *dummyVolumeManager) Detach(name string) error {
	return nil
}

func (d *dummyVolumeManager) UpdateSchedule(name string, jobs []*types.RecurringJob) error {
	return nil
}

func (d *dummyVolumeManager) CheckController(ctrl types.Controller, volume *types.VolumeInfo) error {
	return nil
}

func (d *dummyVolumeManager) Cleanup(volume *types.VolumeInfo) error {
	return nil
}

func (d *dummyVolumeManager) SnapshotOps(name string) (types.SnapshotOps, error) {
	return &dummySnapshots{}, nil
}

func (d *dummyVolumeManager) VolumeBackupOps(name string) (types.VolumeBackupOps, error) {
	return nil, nil
}

func (d *dummyVolumeManager) Controller(name string) (types.Controller, error) {
	return nil, nil
}

func (d *dummyVolumeManager) Settings() types.Settings {
	return &dummySettings{}
}

func (d *dummyVolumeManager) ManagerBackupOps(backupTarget string) types.ManagerBackupOps {
	panic("implement me")
}

type dummySettings struct {
}

func (d *dummySettings) GetSettings() (*types.SettingsInfo, error) {
	return &types.SettingsInfo{}, nil
}

func (d *dummySettings) SetSettings(s *types.SettingsInfo) error {
	return nil
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

func (l *dummyLocator) GetCurrentHostID() string {
	logrus.Infof("Dummy SL: get current hostID: '%s' : %v", l.thisHostID)
	return l.thisHostID
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

func (d *dummySnapshots) Backup(name, backupTarget string) error {
	return nil
}

func (d *dummySnapshots) Purge() error {
	return nil
}

func (d *dummyVolumeManager) ListHosts() (map[string]*types.HostInfo, error) {
	return nil, nil
}

func (d *dummyVolumeManager) GetHost(id string) (*types.HostInfo, error) {
	return nil, nil
}
