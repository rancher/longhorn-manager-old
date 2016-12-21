package storagepool

import (
	"github.com/Sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/rancher/kubernetes-agent/healthcheck"
	"github.com/rancher/longhorn-orc/cattle"
	"github.com/rancher/longhorn-orc/cattleevents"
	"github.com/rancher/longhorn-orc/util"
)

const healthCheckPort = 10241

var Command = cli.Command{
	Name:   "storagepool",
	Usage:  "Start convoy-agent as a storagepool agent",
	Action: start,
}

func start(c *cli.Context) {
	go func() {
		err := healthcheck.StartHealthCheck(healthCheckPort)
		logrus.Fatalf("Error while running healthcheck [%v]", err)
	}()

	healthCheckInterval := c.GlobalInt("healthcheck-interval")

	cattleURL := c.GlobalString("cattle-url")
	cattleAccessKey := c.GlobalString("cattle-access-key")
	cattleSecretKey := c.GlobalString("cattle-secret-key")

	cattleClient, err := cattle.NewCattleClient(cattleURL, cattleAccessKey, cattleSecretKey)
	if err != nil {
		logrus.Fatal(err)
	}

	metadataURL := c.GlobalString("metadata-url")
	md, err := util.GetMetadataConfig(metadataURL)
	if err != nil {
		logrus.Fatalf("Unable to get metadata: %v", err)
	}

	resultChan := make(chan error)

	go func(rc chan error) {
		storagePoolAgent := NewStoragepoolAgent(healthCheckInterval, md.DriverName, cattleClient)
		err := storagePoolAgent.Run(metadataURL)
		logrus.Errorf("Error while running storage pool agent [%v]", err)
		rc <- err
	}(resultChan)

	go func(rc chan error) {
		conf := cattleevents.Config{
			CattleURL:       cattleURL,
			CattleAccessKey: cattleAccessKey,
			CattleSecretKey: cattleSecretKey,
			WorkerCount:     10,
		}
		err := cattleevents.ConnectToEventStream(conf)
		logrus.Errorf("Cattle event listener exited with error: %s", err)
		rc <- err
	}(resultChan)

	<-resultChan
}
