package tests

import (
	//"os"
	"testing"

	//"github.com/docker/go-plugins-helpers/volume"
	"gopkg.in/check.v1"
	//	_ "github.com/fsouza/go-dockerclient"
	//
	//	"github.com/rancher/docker-longhorn-driver/docker/volumeplugin"
	//	"github.com/rancher/docker-longhorn-driver/driver"
)

const baseURL = "http://localhost/v1/"

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	check.TestingT(t)
}

type TestSuite struct {
}

var _ = check.Suite(&TestSuite{})

func (s *TestSuite) SetUpSuite(c *check.C) {
	c.Log("OK")
	//
	//sd := driver.NewStorageDaemon()
	//d := volumeplugin.NewRancherStorageDriver(sd)
	//h := volume.NewHandler(d)
	//c.Logf("Launching test plugin")
	//h.ServeUnix("root", "/host/var/run/mytest.sock")
}

func (s *TestSuite) TestDaemon3(c *check.C) {
	//sock := os.Getenv("DOCKER_TEST_SOCKET")
	//c.Logf("THE SOCKET: %v", sock)
}
