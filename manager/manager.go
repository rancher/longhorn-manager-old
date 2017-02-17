package manager

import (
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-orc/types"
	"io"
	"sync"
	"time"
)

var (
	KeepBadReplicasPeriod = time.Hour * 2
)

type volumeManager struct {
	sync.Mutex

	monitors       map[string]io.Closer
	addingReplicas map[string]int

	orc     types.Orchestrator
	monitor types.Monitor
}

func New(orc types.Orchestrator, monitor types.Monitor) types.VolumeManager {
	return &volumeManager{
		monitors:       map[string]io.Closer{},
		addingReplicas: map[string]int{},

		orc:     orc,
		monitor: monitor,
	}
}

func (man *volumeManager) Create(volume *types.VolumeInfo) (*types.VolumeInfo, error) {
	vol, err := man.orc.CreateVolume(volume)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create volume '%s'", volume.Name)
	}
	return vol, nil
}

func (man *volumeManager) Delete(name string) error {
	return errors.Wrapf(man.orc.DeleteVolume(name), "failed to delete volume '%s'", name)
}

func volumeState(volume *types.VolumeInfo) types.VolumeState {
	goodReplicaCount := 0
	for _, replica := range volume.Replicas {
		if replica.BadTimestamp == nil {
			goodReplicaCount++
		}
	}
	switch {
	case goodReplicaCount == 0:
		return types.Faulted
	case volume.Controller == nil:
		return types.Detached
	case goodReplicaCount == volume.NumberOfReplicas:
		return types.Healthy
	}
	return types.Degraded
}

func (man *volumeManager) Get(name string) (*types.VolumeInfo, error) {
	vol, err := man.orc.GetVolume(name)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get volume '%s'", name)
	}

	state := volumeState(vol)
	vol.State = &state

	return vol, nil
}

func (man *volumeManager) startMonitoring(volume *types.VolumeInfo) {
	man.Lock()
	defer man.Unlock()
	if man.monitors[volume.Name] == nil {
		man.monitors[volume.Name] = man.monitor(volume, man)
	}
}

func (man *volumeManager) stopMonitoring(volume *types.VolumeInfo) {
	man.Lock()
	defer man.Unlock()
	if mon := man.monitors[volume.Name]; mon != nil {
		mon.Close()
		delete(man.monitors, volume.Name)
	}
}

func (man *volumeManager) Attach(name string) error {
	volume, err := man.Get(name)
	if err != nil {
		return err
	}
	if volume.Controller != nil && volume.Controller.Running {
		if volume.Controller.HostID == man.orc.GetThisHostID() {
			man.startMonitoring(volume)
			return nil
		} else if err := man.Detach(name); err != nil {
			return errors.Wrapf(err, "failed to detach before reattaching volume '%s'", name)
		}
	}
	replicas := map[string]*types.ReplicaInfo{}
	var recentBadReplica *types.ReplicaInfo
	var recentBadK string
	wg := &sync.WaitGroup{}
	errCh := make(chan error)
	for k, replica := range volume.Replicas {
		if replica.Running {
			wg.Add(1)
			go func(replica *types.ReplicaInfo) {
				defer wg.Done()
				if err := man.orc.StopReplica(replica.ID); err != nil {
					errCh <- errors.Wrapf(err, "failed to stop replica '%s' for volume '%s'", replica.Name, volume.Name)
				}
			}(replica)
		}
		if replica.BadTimestamp == nil {
			replicas[k] = replica
		} else if recentBadReplica == nil || replica.BadTimestamp.After(*recentBadReplica.BadTimestamp) {
			recentBadReplica = replica
			recentBadK = k
		}
	}
	go func() {
		wg.Wait()
		close(errCh)
	}()
	errs := Errs{}
	for err := range errCh {
		errs = append(errs, err)
		logrus.Errorf("%+v", err)
	}
	if len(errs) > 0 {
		return errs
	}
	if len(replicas) == 0 && recentBadReplica != nil {
		replicas[recentBadK] = recentBadReplica
	}
	if len(replicas) == 0 {
		return errors.Errorf("no replicas to start the controller for volume '%s'", volume.Name)
	}
	wg = &sync.WaitGroup{}
	errCh = make(chan error)
	for _, replica := range replicas {
		wg.Add(1)
		go func(replica *types.ReplicaInfo) {
			defer wg.Done()
			if err := man.orc.StartReplica(replica.ID); err != nil {
				errCh <- errors.Wrapf(err, "failed to start replica '%s' for volume '%s'", replica.Name, volume.Name)
			}
		}(replica)
	}
	go func() {
		wg.Wait()
		close(errCh)
	}()
	errs = Errs{}
	for err := range errCh {
		errs = append(errs, err)
		logrus.Errorf("%+v", err)
	}
	if len(errs) > 0 {
		return errs
	}

	controllerInfo, err := man.orc.CreateController(volume.Name, replicas)
	if err != nil {
		return errors.Wrapf(err, "failed to start the controller for volume '%s'", volume.Name)
	}

	volume.Controller = controllerInfo
	man.startMonitoring(volume)
	return nil
}

func (man *volumeManager) Detach(name string) error {
	volume, err := man.Get(name)
	if err != nil {
		return err
	}
	man.stopMonitoring(volume)
	errCh := make(chan error)
	wg := &sync.WaitGroup{}
	if volume.Controller != nil && volume.Controller.Running {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := man.orc.RemoveInstance(volume.Controller.ID); err != nil {
				errCh <- errors.Wrapf(err, "failed to remove controller '%s' from volume '%s'", volume.Controller.ID, volume.Name)
			}
		}()
	}
	for _, replica := range volume.Replicas {
		wg.Add(1)
		go func(replica *types.ReplicaInfo) {
			defer wg.Done()
			if err := man.orc.StopReplica(replica.ID); err != nil {
				errCh <- errors.Wrapf(err, "failed to stop replica '%s' for volume '%s'", replica.Name, volume.Name)
			}
		}(replica)
	}
	go func() {
		wg.Wait()
		close(errCh)
	}()
	errs := Errs{}
	for err := range errCh {
		errs = append(errs, err)
		logrus.Errorf("%+v", err)
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

func (man *volumeManager) createAndAddReplicaToController(volumeName string, ctrl types.Controller) error {
	replica, err := man.orc.CreateReplica(volumeName)
	if err != nil {
		return errors.Wrapf(err, "failed to create a replica for volume '%s'", volumeName)
	}
	go func() {
		man.addingReplicasCount(volumeName, 1)
		defer man.addingReplicasCount(volumeName, -1)
		if err := ctrl.AddReplica(replica); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "failed to add replica '%s' to volume '%s'", replica.Name, volumeName))
			if err := man.orc.RemoveInstance(replica.ID); err != nil {
				logrus.Errorf("%+v", errors.Wrapf(err, "failed to remove stale replica '%s' of volume '%s'", replica.Name, volumeName))
			}
		}
	}()
	return nil
}

func (man *volumeManager) addingReplicasCount(name string, add int) int {
	man.Lock()
	defer man.Unlock()
	count := man.addingReplicas[name] + add
	man.addingReplicas[name] = count
	return count
}

func (man *volumeManager) CheckController(ctrl types.Controller, volume *types.VolumeInfo) error {
	replicas, err := ctrl.GetReplicaStates()
	if err != nil {
		return NewControllerError(err)
	}
	logrus.Debugf("checking '%s', NumberOfReplicas=%v: controller knows %v replicas", volume.Name, volume.NumberOfReplicas, len(volume.Replicas))
	goodReplicas := []*types.ReplicaInfo{}
	woReplicas := []*types.ReplicaInfo{}
	errCh := make(chan error)
	wg := &sync.WaitGroup{}
	for _, replica := range replicas {
		switch replica.Mode {
		case types.RW:
			goodReplicas = append(goodReplicas, replica)
		case types.WO:
			woReplicas = append(woReplicas, replica)
		case types.ERR:
			wg.Add(1)
			go func(replica *types.ReplicaInfo) {
				defer wg.Done()
				logrus.Warnf("Marking bad replica '%s'", replica.Address)
				wg.Add(2)
				go func() {
					defer wg.Done()
					err := ctrl.RemoveReplica(replica)
					errCh <- errors.Wrapf(err, "failed to remove ERR replica '%s' from volume '%s'", replica.Address, volume.Name)
				}()
				go func() {
					defer wg.Done()
					err := man.orc.MarkBadReplica(volume.Name, replica)
					errCh <- errors.Wrapf(err, "failed to mark replica '%s' bad for volume '%s'", replica.Address, volume.Name)
				}()
			}(replica)
		}
	}
	go func() {
		wg.Wait()
		close(errCh)
	}()
	errs := Errs{}
	for err := range errCh {
		if err == nil {
			continue
		}
		errs = append(errs, err)
		logrus.Errorf("%+v", err)
	}
	if len(errs) > 0 {
		return errs
	}
	if len(goodReplicas) == 0 {
		logrus.Errorf("volume '%s' has no more good replicas, shutting it down", volume.Name)
		return man.Detach(volume.Name)
	}

	addingReplicas := man.addingReplicasCount(volume.Name, 0)
	logrus.Debugf("'%s' replicas by state: RW=%v, WO=%v, adding=%v", volume.Name, len(goodReplicas), len(woReplicas), addingReplicas)
	if len(goodReplicas) < volume.NumberOfReplicas && len(woReplicas) == 0 && addingReplicas == 0 {
		if err := man.createAndAddReplicaToController(volume.Name, ctrl); err != nil {
			return err
		}
	}
	if len(goodReplicas)+len(woReplicas) > volume.NumberOfReplicas {
		logrus.Warnf("volume '%s' has more replicas than needed: has %v, needs %v", volume.Name, len(goodReplicas), volume.NumberOfReplicas)
	}

	return nil
}

func (man *volumeManager) Cleanup(v *types.VolumeInfo) error {
	volume, err := man.Get(v.Name)
	if err != nil {
		return errors.Wrapf(err, "error getting volume '%s'", v.Name)
	}
	logrus.Infof("running cleanup, volume '%s'", volume.Name)
	now := time.Now().UTC()
	errCh := make(chan error)
	wg := &sync.WaitGroup{}
	for _, replica := range volume.Replicas {
		if replica.BadTimestamp == nil {
			continue
		}
		wg.Add(1)
		go func(replica *types.ReplicaInfo) {
			defer wg.Done()
			if replica.Running {
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := man.orc.StopReplica(replica.ID)
					errCh <- errors.Wrapf(err, "error stopping bad replica '%s', volume '%s'", replica.Name, volume.Name)
				}()
			}
			if (*replica.BadTimestamp).Add(KeepBadReplicasPeriod).Before(now) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := man.orc.RemoveInstance(replica.ID)
					errCh <- errors.Wrapf(err, "error removing old bad replica '%s', volume '%s'", replica.Name, volume.Name)
				}()
			}
		}(replica)
	}
	go func() {
		wg.Wait()
		close(errCh)
	}()
	errs := Errs{}
	for err := range errCh {
		if err == nil {
			continue
		}
		errs = append(errs, err)
		logrus.Errorf("%+v", err)
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}
