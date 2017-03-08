package main

import (
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/urfave/cli"

	"fmt"
	"github.com/rancher/longhorn-orc/api"
	"github.com/rancher/longhorn-orc/controller"
	"github.com/rancher/longhorn-orc/manager"
	"github.com/rancher/longhorn-orc/orch"
	"github.com/rancher/longhorn-orc/orch/cattle"
	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util/daemon"
	"github.com/rancher/longhorn-orc/util/server"
)

const (
	sockFile           = "/var/run/rancher/longhorn/volume-manager.sock"
	RancherMetadataURL = "http://rancher-metadata/2016-07-29"
)

var VERSION = "0.1.0"

func main() {
	logrus.SetFormatter(&logrus.TextFormatter{ForceColors: true})

	app := cli.NewApp()
	app.Version = VERSION
	app.Usage = "Rancher Longhorn storage driver/orchestration"
	app.Action = RunManager

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:   "debug, d",
			Usage:  "enable debug logging level",
			EnvVar: "RANCHER_DEBUG",
		},
		cli.StringFlag{
			Name:   "cattle-url",
			Usage:  "The URL endpoint to communicate with cattle server",
			EnvVar: "CATTLE_URL",
		},
		cli.StringFlag{
			Name:   "cattle-access-key",
			Usage:  "The access key required to authenticate with cattle server",
			EnvVar: "CATTLE_ACCESS_KEY",
		},
		cli.StringFlag{
			Name:   "cattle-secret-key",
			Usage:  "The secret key required to authenticate with cattle server",
			EnvVar: "CATTLE_SECRET_KEY",
		},
		cli.StringFlag{
			Name:   orch.LonghornImageParam,
			EnvVar: "LONGHORN_IMAGE",
		},
		cli.IntFlag{
			Name:  "healthcheck-interval",
			Value: 5000,
			Usage: "set the frequency of performing healthchecks",
		},
		cli.StringFlag{
			Name:  "metadata-url",
			Usage: "set the metadata url",
			Value: RancherMetadataURL,
		},
	}

	if err := app.Run(os.Args); err != nil {
		logrus.Fatalf("Critical error: %v", err)
	}

}

func RunManager(c *cli.Context) error {
	if c.Bool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	}

	orc := cattle.New(c)
	man := manager.New(orc, manager.Monitor(controller.New))

	go server.NewUnixServer(sockFile).Serve(api.HandlerLocal(man))

	//man := api.DummyVolumeManager()
	//sl := api.DummyServiceLocator("localhost-ID")
	sl := orc.(types.ServiceLocator)
	proxy := api.Proxy()

	go server.NewTCPServer(fmt.Sprintf(":%v", api.Port)).Serve(api.Handler(man, sl, proxy))

	return daemon.WaitForExit()
}
