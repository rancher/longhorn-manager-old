package scheduler

import (
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-orc/types"
)

type OrcScheduler struct {
	ops types.ScheduleOps
}

func NewOrcScheduler(ops types.ScheduleOps) *OrcScheduler {
	return &OrcScheduler{
		ops: ops,
	}
}

func randomHostID(m map[string]*types.HostInfo) string {
	for k := range m {
		return k
	}
	return ""
}

func (s *OrcScheduler) Schedule(item *types.ScheduleItem) (*types.InstanceInfo, error) {
	if item.Instance.ID == "" {
		return nil, errors.Errorf("instance ID required for scheduling")
	}
	if item.Instance.HostID != "" {
		return s.ScheduleProcess(&types.ScheduleSpec{
			HostID: item.Instance.HostID,
		}, item)
	}

	hosts, err := s.ops.ListHosts()
	if err != nil {
		return nil, errors.Wrap(err, "fail to schedule")
	}

	availableHosts := hosts
	for len(availableHosts) != 0 {
		hostID := randomHostID(availableHosts)

		ret, err := s.ScheduleProcess(&types.ScheduleSpec{HostID: hostID}, item)
		if err == nil {
			return ret, nil
		}

		logrus.Warnf("Fail to schedule %+v, trying on another one: %v", item.Instance, err)
		delete(availableHosts, hostID)

	}
	return nil, errors.Errorf("unable to find suitable host for scheduling")
}

func (s *OrcScheduler) ScheduleProcess(spec *types.ScheduleSpec, item *types.ScheduleItem) (*types.InstanceInfo, error) {
	if s.ops.GetCurrentHostID() == spec.HostID {
		return s.Process(spec, item)
	}

	host, err := s.ops.GetHost(spec.HostID)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot find host %v", spec.HostID)
	}
	client := newSchedulerClient(host)
	ret, err := client.Schedule(item)
	if err != nil {
		return nil, errors.Wrapf(err, "Fail to schedule on host %v(%v %v)", host.UUID, host.Name, host.Address)
	}
	logrus.Debugf("Scheduled %v to %v(%v)", item.Instance.ID, host.UUID, host.Address)
	return ret, nil
}

func (s *OrcScheduler) Process(spec *types.ScheduleSpec, item *types.ScheduleItem) (*types.InstanceInfo, error) {
	if s.ops.GetCurrentHostID() != spec.HostID {
		return nil, errors.Errorf("wrong host routing, should be at %v", spec.HostID)
	}
	instance, err := s.ops.ProcessSchedule(item)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to process schedule request")
	}
	if instance == nil || instance.ID == "" {
		return nil, errors.Errorf("missing key fields from schedule response %+v", instance)
	}
	return instance, nil
}
