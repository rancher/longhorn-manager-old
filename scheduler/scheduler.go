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

func (s *OrcScheduler) Schedule(item *types.ScheduleItem, policy *types.SchedulePolicy) (*types.InstanceInfo, error) {
	if item.Instance.ID == "" || item.Instance.Type == types.InstanceTypeNone {
		return nil, errors.Errorf("instance ID and type required for scheduling")
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

	normalPriorityList := []string{}
	lowPriorityList := []string{}

	for id := range hosts {
		if policy != nil {
			if policy.Binding == types.SchedulePolicyBindingSoftAntiAffinity {
				if _, ok := policy.HostIDMap[id]; ok {
					lowPriorityList = append(lowPriorityList, id)
				} else {
					normalPriorityList = append(normalPriorityList, id)
				}
			} else {
				return nil, errors.Errorf("Unsupported schedule policy binding %v", policy.Binding)
			}
		} else {
			normalPriorityList = append(normalPriorityList, id)
		}
	}

	priorityList := append(normalPriorityList, lowPriorityList...)

	for _, id := range priorityList {
		ret, err := s.ScheduleProcess(&types.ScheduleSpec{HostID: id}, item)
		if err == nil {
			return ret, nil
		}

		logrus.Warnf("Fail to schedule %+v on host %v, trying on another one: %v",
			hosts[id], item.Instance, err)
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
	logrus.Debugf("Scheduled %v %v to %v(%v)", item.Action, item.Instance.ID, host.UUID, host.Address)
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
	if instance == nil || instance.ID == "" || instance.Type == types.InstanceTypeNone {
		return nil, errors.Errorf("missing key fields from schedule response %+v", instance)
	}
	return instance, nil
}
