package docker

import (
	"os"
	"strconv"
	"testing"

	"github.com/rancher/longhorn-orc/types"

	. "gopkg.in/check.v1"
)

const (
	TestVolumeName = "test-vol"

	EnvEtcdServer    = "LONGHORN_ORC_TEST_ETCD_SERVER"
	EnvLonghornImage = "LONGHORN_IMAGE"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	d             *dockerOrc
	longhornImage string

	replicaBin    map[string]struct{}
	controllerBin map[string]struct{}
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
	var err error

	s.replicaBin = make(map[string]struct{})
	s.controllerBin = make(map[string]struct{})

	etcdIP := os.Getenv(EnvEtcdServer)
	c.Assert(etcdIP, Not(Equals), "")

	s.longhornImage = os.Getenv(EnvLonghornImage)
	c.Assert(s.longhornImage, Not(Equals), "")

	cfg := &dockerOrcConfig{
		servers: []string{"http://" + etcdIP + ":2379"},
		prefix:  "/longhorn",
	}
	orc, err := newDocker(cfg)
	s.d = orc.(*dockerOrc)
	c.Assert(err, IsNil)
}

func (s *TestSuite) Cleanup() {
	for id := range s.replicaBin {
		s.d.stopInstance(id, types.InstanceTypeReplica)
		s.d.removeInstance(id, types.InstanceTypeReplica)
	}
	for id := range s.controllerBin {
		s.d.stopInstance(id, types.InstanceTypeController)
		s.d.removeInstance(id, types.InstanceTypeController)
	}
}

func (s *TestSuite) TestCreateVolume(c *C) {
	var instance *types.InstanceInfo

	defer s.Cleanup()

	volume := &types.VolumeInfo{
		Name:          TestVolumeName,
		Size:          8 * 1024 * 1024, // 8M
		LonghornImage: s.longhornImage,
	}
	replica1Data := &dockerScheduleData{
		VolumeName:    volume.Name,
		VolumeSize:    strconv.FormatInt(volume.Size, 10),
		InstanceName:  "replica-test-1",
		LonghornImage: volume.LonghornImage,
	}
	replica1, err := s.d.createReplica(replica1Data)
	c.Assert(err, IsNil)
	c.Assert(replica1.ID, NotNil)
	s.replicaBin[replica1.ID] = struct{}{}

	c.Assert(replica1.HostID, Equals, s.d.GetCurrentHostID())
	c.Assert(replica1.Running, Equals, true)
	c.Assert(replica1.Name, Equals, replica1Data.InstanceName)

	instance, err = s.d.stopInstance(replica1.ID, types.InstanceTypeReplica)
	c.Assert(err, IsNil)
	c.Assert(instance.ID, Equals, replica1.ID)
	c.Assert(instance.Name, Equals, replica1.Name)
	c.Assert(instance.Running, Equals, false)

	instance, err = s.d.startInstance(replica1.ID, types.InstanceTypeReplica)
	c.Assert(err, IsNil)
	c.Assert(instance.ID, Equals, replica1.ID)
	c.Assert(instance.Name, Equals, replica1.Name)
	c.Assert(instance.Running, Equals, true)

	replica2Data := &dockerScheduleData{
		VolumeName:    volume.Name,
		VolumeSize:    strconv.FormatInt(volume.Size, 10),
		InstanceName:  "replica-test-2",
		LonghornImage: volume.LonghornImage,
	}
	replica2, err := s.d.createReplica(replica2Data)
	c.Assert(err, IsNil)
	c.Assert(replica2.ID, NotNil)
	s.replicaBin[replica2.ID] = struct{}{}

	controllerName := "controller-test"

	data := &dockerScheduleData{
		VolumeName:    volume.Name,
		InstanceName:  controllerName,
		LonghornImage: volume.LonghornImage,
		ReplicaAddresses: []string{
			"tcp://" + replica1.Address + ":9502",
			"tcp://" + replica2.Address + ":9502",
		},
	}
	controller, err := s.d.createController(data)
	c.Assert(err, IsNil)
	c.Assert(controller.ID, NotNil)
	s.controllerBin[controller.ID] = struct{}{}

	c.Assert(controller.HostID, Equals, s.d.GetCurrentHostID())
	c.Assert(controller.Running, Equals, true)
	c.Assert(controller.Name, Equals, controllerName)

	instance, err = s.d.stopInstance(controller.ID, types.InstanceTypeController)
	c.Assert(err, IsNil)
	c.Assert(instance.ID, Equals, controller.ID)
	c.Assert(instance.Name, Equals, controller.Name)
	c.Assert(instance.Running, Equals, false)

	instance, err = s.d.stopInstance(replica1.ID, types.InstanceTypeReplica)
	c.Assert(err, IsNil)
	c.Assert(instance.ID, Equals, replica1.ID)
	c.Assert(instance.Name, Equals, replica1.Name)
	c.Assert(instance.Running, Equals, false)

	instance, err = s.d.stopInstance(replica2.ID, types.InstanceTypeReplica)
	c.Assert(err, IsNil)
	c.Assert(instance.ID, Equals, replica2.ID)
	c.Assert(instance.Name, Equals, replica2.Name)
	c.Assert(instance.Running, Equals, false)

	instance, err = s.d.removeInstance(controller.ID, types.InstanceTypeController)
	c.Assert(err, IsNil)
	c.Assert(instance.ID, Equals, controller.ID)
	delete(s.controllerBin, controller.ID)

	instance, err = s.d.removeInstance(replica1.ID, types.InstanceTypeReplica)
	c.Assert(err, IsNil)
	c.Assert(instance.ID, Equals, replica1.ID)
	delete(s.replicaBin, replica1.ID)

	instance, err = s.d.removeInstance(replica2.ID, types.InstanceTypeReplica)
	c.Assert(err, IsNil)
	c.Assert(instance.ID, Equals, replica2.ID)
	delete(s.replicaBin, replica2.ID)
}
