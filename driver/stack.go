package driver

import (
	"fmt"
	"text/template"
	"time"

	"bytes"
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	rancherClient "github.com/rancher/go-rancher/v2"
	"github.com/rancher/longhorn-orc/util"
)

const (
	retryInterval          = 2 * time.Second
	retryMax               = 1800
	composeAffinityLabel   = "io.rancher.scheduler.affinity:container"
	composeVolumeName      = "VOLUME_NAME"
	composeVolumeSize      = "VOLUME_SIZE"
	composeDriverContainer = "DRIVER_CONTAINER"
	composeImage           = "IMAGE"
	orcImage               = "ORC_IMAGE"
)

var composeTemplate *template.Template

func init() {
	tmplt, err := template.New("compose").Parse(DockerComposeTemplate)
	if err != nil {
		logrus.Panicf("Error parsing compose template: %v", err)
	}

	composeTemplate = tmplt
}

type stack struct {
	rancherClient       *rancherClient.RancherClient
	externalID          string
	name                string
	environment         map[string]interface{}
	driverContainerName string
	volumeConfig        *volumeConfig
}

func newStack(d *StorageDaemon, volumeConfig *volumeConfig) *stack {
	env := map[string]interface{}{
		composeImage:           d.mdc.Image,
		orcImage:               d.mdc.OrcImage,
		composeVolumeName:      volumeConfig.Name,
		composeVolumeSize:      volumeConfig.Size,
		composeDriverContainer: d.mdc.DriverContainerName,
	}

	return &stack{
		rancherClient:       d.rClient,
		name:                util.VolumeToStackName(volumeConfig.Name),
		externalID:          fmt.Sprintf("system://%s?name=%s", d.mdc.DriverName, volumeConfig.Name),
		environment:         env,
		driverContainerName: d.mdc.DriverContainerName,
		volumeConfig:        volumeConfig,
	}
}

func (s *stack) Create() (*rancherClient.Stack, error) {
	stc, err := s.Find()
	if err != nil {
		return nil, err
	}

	if stc == nil {
		dockerCompose := new(bytes.Buffer)
		if err := composeTemplate.Execute(dockerCompose, s.volumeConfig); err != nil {
			return nil, fmt.Errorf("Error generating docker compose: %v", err)
		}

		opts := &rancherClient.Stack{
			Name:          s.name,
			ExternalId:    s.externalID,
			Environment:   s.environment,
			DockerCompose: dockerCompose.String(),
			StartOnCreate: true,
		}
		stc, err = s.rancherClient.Stack.Create(opts)
		if err != nil {
			return nil, err
		}
	}

	if err := WaitStack(s.rancherClient, stc); err != nil {
		return nil, err
	}

	if err := s.waitForServices(stc, "active"); err != nil {
		logrus.Debugf("Failed waiting services to be ready to launch. Cleaning up %v", stc.Name)
		if err := s.rancherClient.Stack.Delete(stc); err != nil {
			return nil, err
		}
	}

	return stc, nil
}

func (s *stack) Delete() error {
	stc, err := s.Find()
	if err != nil || stc == nil {
		return err
	}

	if err := s.rancherClient.Stack.Delete(stc); err != nil {
		return err
	}

	return WaitStack(s.rancherClient, stc)
}

func (s *stack) findController(stc *rancherClient.Stack) ([]rancherClient.Service, error) {
	services, err := s.rancherClient.Service.List(&rancherClient.ListOpts{
		Filters: map[string]interface{}{
			"stackId": stc.Id,
			"name":    "controller",
		},
	})
	if err != nil {
		return nil, err
	}
	return services.Data, nil
}

func (s *stack) StopController() error {
	stc, err := s.Find()
	if err != nil || stc == nil {
		return err
	}

	services, err := s.findController(stc)
	if err != nil {
		return err
	}

	for _, svc := range services {
		if _, err := s.rancherClient.Service.ActionDeactivate(&svc); err != nil {
			return err
		}
	}

	return WaitStack(s.rancherClient, stc)
}

func (s *stack) Find() (*rancherClient.Stack, error) {
	stcs, err := s.rancherClient.Stack.List(&rancherClient.ListOpts{
		Filters: map[string]interface{}{
			"name":         s.name,
			"externalId":   s.externalID,
			"removed_null": nil,
		},
	})
	if err != nil {
		return nil, err
	}
	if len(stcs.Data) == 0 {
		return nil, nil
	}
	if len(stcs.Data) > 1 {
		// This really shouldn't ever happen
		return nil, fmt.Errorf("More than one stack found for %s", s.name)
	}

	return &stcs.Data[0], nil
}

func (s *stack) ensureControllerActive(controller *rancherClient.Service) error {
	if err := WaitService(s.rancherClient, controller); err != nil {
		return err
	}
	if controller.State == "inactive" {
		controller, err := s.rancherClient.Service.ActionActivate(controller)
		if err != nil {
			return errors.Wrap(err, "Failed to activage controller")
		}
		return errors.Wrap(WaitService(s.rancherClient, controller), "Error waiting: controller activate")
	}
	return nil
}

func (s *stack) ensureControllerDoneUpgrading(controller *rancherClient.Service) error {
	util.Backoff(5*time.Minute, fmt.Sprintf("Failed waiting for controller to upgrade: %s", s.name), func() (bool, error) {
		logrus.Infof("waiting for controller to upgrade: %s", s.name)
		err := WaitService(s.rancherClient, controller)
		return controller.State == "upgraded", err
	})
	s.rancherClient.Service.ActionFinishupgrade(controller)
	logrus.Infof("finish upgrade: %s", s.name)
	return errors.Wrap(WaitService(s.rancherClient, controller), "Error waiting: controller finish upgrade")
}

func (s *stack) StartController() error {
	stc, err := s.Find()
	if err != nil {
		return err
	}

	services, err := s.findController(stc)
	if err != nil {
		return err
	}

	if len(services) != 1 {
		return fmt.Errorf("Could not find controller for volume %s", s.volumeConfig.Name)
	}
	controller := &services[0]

	// make sure the controller is in a stable state
	if err := WaitService(s.rancherClient, controller); err != nil {
		return err
	}

	// if controller is on this host
	if controller.LaunchConfig.Labels[composeAffinityLabel] == s.driverContainerName {
		return s.ensureControllerActive(controller)
	}

	// if controller is on another host
	if controller.State != "inactive" {
		return fmt.Errorf("Volume %v is already attached and cannot be mounted", s.volumeConfig.Name)
	}

	newLaunchConfig := controller.LaunchConfig
	newLaunchConfig.Labels[composeAffinityLabel] = s.driverContainerName

	logrus.Infof("Moving controller to next to container %s", s.driverContainerName)
	_, err = s.rancherClient.Service.ActionUpgrade(controller, &rancherClient.ServiceUpgrade{
		InServiceStrategy: &rancherClient.InServiceUpgradeStrategy{
			LaunchConfig: newLaunchConfig,
		},
	})
	if err != nil {
		return errors.Wrap(err, "Failed to upgrade controller")
	}
	logrus.Infof("Finishing controller upgrade...")
	return s.ensureControllerDoneUpgrading(controller)
}

func (s *stack) waitForServices(stc *rancherClient.Stack, targetState string) error {
	var serviceCollection rancherClient.ServiceCollection
	ready := false

	if err := s.rancherClient.GetLink(stc.Resource, "services", &serviceCollection); err != nil {
		return err
	}
	targetServiceCount := len(serviceCollection.Data)

	for i := 0; !ready && i < retryMax; i++ {
		logrus.Debugf("Waiting for %v services in %v turn to %v state", targetServiceCount, stc.Name, targetState)
		time.Sleep(retryInterval)
		if err := s.rancherClient.GetLink(stc.Resource, "services", &serviceCollection); err != nil {
			return err
		}
		services := serviceCollection.Data
		if len(services) != targetServiceCount {
			continue
		}
		incorrectState := false
		for _, service := range services {
			if service.State != targetState {
				incorrectState = true
				break
			}
		}
		if incorrectState {
			continue
		}
		ready = true
	}
	if !ready {
		return fmt.Errorf("Failed to wait for %v services in %v turn to %v state", targetServiceCount, stc.Name, targetState)
	}
	logrus.Debugf("Services change state to %v in %v", targetState, stc.Name)
	return nil
}
