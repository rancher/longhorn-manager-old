package docker

import (
	"os"
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

	containerBin map[string]struct{}
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
	var err error

	s.containerBin = make(map[string]struct{})

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
	for id := range s.containerBin {
		s.d.StopInstance(id)
		s.d.RemoveInstance(id)
	}
}

func (s *TestSuite) TestCreateVolume(c *C) {
	defer s.Cleanup()

	volume := &types.VolumeInfo{
		Name:          TestVolumeName,
		Size:          8 * 1024 * 1024, // 8M
		LonghornImage: s.longhornImage,
	}
	replica1Name := "replica-test-1"
	replica1, err := s.d.createReplica(volume, replica1Name)
	c.Assert(err, IsNil)
	c.Assert(replica1.ID, NotNil)
	s.containerBin[replica1.ID] = struct{}{}

	c.Assert(replica1.HostID, Equals, s.d.GetCurrentHostID())
	c.Assert(replica1.Running, Equals, true)
	c.Assert(replica1.Name, Equals, replica1Name)

	err = s.d.StopInstance(replica1.ID)
	c.Assert(err, IsNil)

	err = s.d.StartInstance(replica1.ID)
	c.Assert(err, IsNil)

	replica2Name := "replica-test-2"
	replica2, err := s.d.createReplica(volume, replica2Name)
	c.Assert(err, IsNil)
	c.Assert(replica2.ID, NotNil)
	s.containerBin[replica2.ID] = struct{}{}

	replicas := map[string]*types.ReplicaInfo{
		replica1.Name: replica1,
		replica2.Name: replica2,
	}
	controller, err := s.d.createController(volume, replicas)
	c.Assert(err, IsNil)
	c.Assert(controller.ID, NotNil)
	s.containerBin[controller.ID] = struct{}{}

	c.Assert(controller.HostID, Equals, s.d.GetCurrentHostID())
	c.Assert(controller.Running, Equals, true)

	err = s.d.StopInstance(controller.ID)
	c.Assert(err, IsNil)
	err = s.d.StopInstance(replica1.ID)
	c.Assert(err, IsNil)
	err = s.d.StopInstance(replica2.ID)
	c.Assert(err, IsNil)

	err = s.d.RemoveInstance(controller.ID)
	c.Assert(err, IsNil)
	delete(s.containerBin, controller.ID)
	err = s.d.RemoveInstance(replica1.ID)
	c.Assert(err, IsNil)
	delete(s.containerBin, replica1.ID)
	err = s.d.RemoveInstance(replica2.ID)
	c.Assert(err, IsNil)
	delete(s.containerBin, replica2.ID)
}
