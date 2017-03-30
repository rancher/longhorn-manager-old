package docker

import (
	"testing"

	"github.com/rancher/longhorn-orc/types"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	d            *dockerOrc
	containerBin map[string]struct{}
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
	var err error

	s.containerBin = make(map[string]struct{})

	cfg := &dockerOrcConfig{
		servers: []string{"http://localhost:2379"},
		address: "127.0.0.1",
		prefix:  "/longhorn",
		image:   "rancher/longhorn:latest",
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

func (s *TestSuite) TestCreateReplica(c *C) {
	defer s.Cleanup()

	volume := &types.VolumeInfo{
		Size: 1024 * 1024 * 1024, // 1G
	}
	replica1Name := "replica-test-1"
	replica1, err := s.d.createReplica(replica1Name, volume)
	c.Assert(err, IsNil)
	c.Assert(replica1.ID, NotNil)
	s.containerBin[replica1.ID] = struct{}{}

	c.Assert(replica1.HostID, Equals, s.d.GetCurrentHostID())
	c.Assert(replica1.Running, Equals, true)
	// It's weird that Docker put a forward slash to the container name
	// So it become "/replica-test-1"
	c.Assert(replica1.Name, Equals, "/"+replica1Name)

	err = s.d.StopInstance(replica1.ID)
	c.Assert(err, IsNil)

	err = s.d.StartInstance(replica1.ID)
	c.Assert(err, IsNil)

	err = s.d.StopInstance(replica1.ID)
	c.Assert(err, IsNil)

	err = s.d.RemoveInstance(replica1.ID)
	c.Assert(err, IsNil)

	delete(s.containerBin, replica1.ID)
}
