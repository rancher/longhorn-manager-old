package storagepool

import (
	"time"

	"github.com/rancher/go-rancher-metadata/metadata"
)

type healthChecker interface {
	populateHostMap() (map[string]string, error)
}

func newHealthChecker(metadataURL string) (healthChecker, error) {
	return &metadataBasedHealthCheck{
		metadataURL: metadataURL,
	}, nil
}

type metadataBasedHealthCheck struct {
	version     string
	prevHosts   map[string]string
	metadataURL string
}

func (mt *metadataBasedHealthCheck) populateHostMap() (map[string]string, error) {
	m := metadata.NewClient(mt.metadataURL)

	version, err := m.GetVersion()
	if err != nil {
		return nil, err
	}
	if version == mt.version {
		return mt.prevHosts, nil
	}

	activeHosts := map[string]string{}
	timeStamp := time.Now().Format(time.RFC1123Z)
	stack, err := m.GetSelfStack()
	if err != nil {
		return nil, err
	}

	for _, svc := range stack.Services {
		for _, c := range svc.Containers {
			activeHosts[c.HostUUID] = timeStamp
		}
	}

	mt.prevHosts = activeHosts
	mt.version = version
	return activeHosts, nil
}
