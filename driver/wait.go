package driver

import (
	"fmt"
	"time"

	rancherClient "github.com/rancher/go-rancher/v2"
	"github.com/rancher/longhorn-orc/util"
)

func WaitFor(client *rancherClient.RancherClient, resource *rancherClient.Resource, output interface{}, transitioning func() string) error {
	return util.Backoff(60*time.Minute, fmt.Sprintf("Failed waiting for %s:%s", resource.Type, resource.Id), func() (bool, error) {
		err := client.Reload(resource, output)
		if err != nil {
			return false, err
		}
		if transitioning() != "yes" {
			return true, nil
		}
		return false, nil
	})
}

func WaitService(client *rancherClient.RancherClient, service *rancherClient.Service) error {
	return WaitFor(client, &service.Resource, service, func() string {
		return service.Transitioning
	})
}

func WaitStack(client *rancherClient.RancherClient, stc *rancherClient.Stack) error {
	return WaitFor(client, &stc.Resource, stc, func() string {
		return stc.Transitioning
	})
}
