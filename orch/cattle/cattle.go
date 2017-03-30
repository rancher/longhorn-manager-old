package cattle

import (
	"bytes"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/docker/distribution/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher-metadata/metadata"
	client "github.com/rancher/go-rancher/v2"
	"github.com/rancher/longhorn-orc/orch"
	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
	"github.com/urfave/cli"
	"net/http"
	"sync"
	"text/template"
	"time"
)

const (
	replicaNamePrefix = "replica"
	volumeProperty    = "volume"
)

var (
	dockerComposeTemplate  *template.Template
	rancherComposeTemplate *template.Template

	templateFuncs = template.FuncMap{
		"StackName": util.VolumeStackName,
	}
)

func init() {
	t, err := template.New("docker-compose").Funcs(templateFuncs).Parse(dockerComposeText)
	if err != nil {
		logrus.Fatalf("Error parsing volume stack template: %v", err)
	}
	dockerComposeTemplate = t

	t, err = template.New("rancher-compose").Parse(rancherComposeText)
	if err != nil {
		logrus.Fatalf("Error parsing volume stack template: %v", err)
	}
	rancherComposeTemplate = t
}

type cattleOrc struct {
	sync.Mutex

	rancher  *client.RancherClient
	metadata metadata.Client
	dragon   *hiddenDragon

	hostUUID, containerUUID string

	LonghornImage string // TODO should be replaced by volume.LonghornImage
}

func New(c *cli.Context) (types.Orchestrator, error) {
	clientOpts := &client.ClientOpts{
		Url:       c.GlobalString("cattle-url"),
		AccessKey: c.GlobalString("cattle-access-key"),
		SecretKey: c.GlobalString("cattle-secret-key"),
	}
	rancherClient, err := client.NewRancherClient(clientOpts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get rancher client")
	}
	logrus.Debugf("rancher clientOpts: %+v", *clientOpts)

	md := metadata.NewClient(c.GlobalString("metadata-url"))
	host, err := md.GetSelfHost()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get self host from rancher metadata")
	}
	logrus.Infof("cattle orc: this host UUID: '%s'", host.UUID)

	container, err := md.GetSelfContainer()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get self container from rancher metadata")
	}
	logrus.Infof("cattle orc: this container UUID: '%s'", container.UUID)

	return &cattleOrc{
		rancher:       rancherClient,
		metadata:      md,
		dragon:        &hiddenDragon{httpClient: &http.Client{Timeout: 10 * time.Second}},
		hostUUID:      host.UUID,
		containerUUID: container.UUID,
		LonghornImage: c.GlobalString(orch.LonghornImageParam),
	}, nil
}

func volumeStackExternalID(name string) string {
	return fmt.Sprintf("system://%s?name=%s", "rancher-longhorn", name)
}

func replicaName(index string) string {
	return "replica-" + index
}

func replicaIndex(name string) string {
	return name[len(replicaNamePrefix)+1:]
}

func stackBuffer(t *template.Template, volume *types.VolumeInfo) *bytes.Buffer {
	buffer := new(bytes.Buffer)
	if err := t.Execute(buffer, volume); err != nil {
		logrus.Fatalf("Error applying the stack golang template: %v", err)
	}
	logrus.Debugf("%s", buffer)
	return buffer
}

func copyVolumeProperties(volume0 *types.VolumeInfo) *types.VolumeInfo {
	volume := new(types.VolumeInfo)
	*volume = *volume0
	volume.Controller = nil
	volume.Replicas = nil
	volume.State = 0
	return volume
}

func randStr() string {
	return uuid.Generate().String()[:18]
}

func genReplicas(numberOfReplicas int) map[string]*types.ReplicaInfo {
	replicas := map[string]*types.ReplicaInfo{}
	replicaNames := make([]string, numberOfReplicas)
	for i := 0; i < numberOfReplicas; i++ {
		index := randStr()
		name := replicaName(index)
		replicas[index] = &types.ReplicaInfo{Name: name}
		replicaNames[i] = name
	}
	return replicas
}

func (orc *cattleOrc) createVolume(volume *types.VolumeInfo) (*types.VolumeInfo, error) {
	stack0 := &client.Stack{
		Name:           util.VolumeStackName(volume.Name),
		ExternalId:     volumeStackExternalID(volume.Name),
		DockerCompose:  stackBuffer(dockerComposeTemplate, volume).String(),
		RancherCompose: stackBuffer(rancherComposeTemplate, volume).String(),
		StartOnCreate:  true,
	}
	stack, err := orc.rancher.Stack.Create(stack0)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create stack '%s'", stack0.Name)
	}
	volume.Replicas = genReplicas(volume.NumberOfReplicas)

	for _, replica := range volume.Replicas {
		_, err := orc.rancher.Container.Create(orc.replicaContainer(volume, replica))
		if err != nil {
			return nil, errors.Wrapf(err, "error creating replica '%s', volume '%s'", replica.Name, volume.Name)
		}
	}

	if err := util.Backoff(30*time.Second, "timed out", func() (bool, error) {
		s, err := orc.rancher.Stack.ById(stack.Id)
		if err != nil {
			return false, errors.Wrapf(err, "error getting stack info, volume '%s'", volume.Name)
		}
		if s.State == "active" {
			return true, nil
		}
		return false, nil
	}); err != nil {
		return nil, errors.Wrap(err, "error waiting for volume stack")
	}

	return orc.GetVolume(volume.Name)
}

func (orc *cattleOrc) CreateVolume(volume *types.VolumeInfo) (*types.VolumeInfo, error) {
	if v, err := orc.GetVolume(volume.Name); err != nil {
		return nil, err
	} else if v != nil {
		return v, nil
	}
	return orc.createVolume(copyVolumeProperties(volume))
}

func (orc *cattleOrc) DeleteVolume(volumeName string) error {
	stack, err := orc.getStack(volumeName)
	if err != nil {
		return err
	}
	if stack == nil {
		return nil
	}
	volume, err := orc.getVolume(volumeName, stack)
	if err != nil {
		return err
	}
	if volume.Controller != nil {
		cnt, err := orc.rancher.Container.ById(volume.Controller.ID)
		if err != nil {
			return errors.Wrapf(err, "error getting controller container (for delete), volume '%s'", volumeName)
		}
		if err := orc.rancher.Container.Delete(cnt); err != nil {
			return errors.Wrapf(err, "error deleting controller container, volume '%s'", volumeName)
		}
	}
	for _, replica := range volume.Replicas {
		cnt, err := orc.rancher.Container.ById(replica.ID)
		if err != nil {
			return errors.Wrapf(err, "error getting replica container (for delete) '%s', volume '%s'", replica.Name, volumeName)
		}
		if err := orc.rancher.Container.Delete(cnt); err != nil {
			return errors.Wrapf(err, "error deleting controller container '%s', volume '%s'", replica.Name, volumeName)
		}
	}
	_, err = orc.rancher.Stack.ActionRemove(stack)
	return errors.Wrapf(err, "error removing stack for volume '%s'", volumeName)
}

func (orc *cattleOrc) getStack(volumeName string) (*client.Stack, error) {
	stackColl, err := orc.rancher.Stack.List(&client.ListOpts{Filters: map[string]interface{}{
		"name":         util.VolumeStackName(volumeName),
		"externalId":   volumeStackExternalID(volumeName),
		"removed_null": nil,
	}})
	if err != nil {
		return nil, errors.Wrap(err, "error listing stacks")
	}
	if len(stackColl.Data) == 0 {
		return nil, nil
	}
	return &stackColl.Data[0], nil
}

type replicaMd struct {
	BadTimestamp *string
}

func (orc *cattleOrc) getReplicaBadTS(replicaName, volumeName string, svc *client.Service) (*time.Time, error) {
	var md replicaMd
	data := svc.Metadata[replicaName]
	if data == nil {
		return nil, nil
	}
	if err := mapstructure.Decode(data, &md); err != nil {
		return nil, errors.Wrapf(err, "error parsing metadata, replica '%s', volume '%s'", replicaName, volumeName)
	}

	if md.BadTimestamp != nil {
		ts, err := util.ParseTimeZ(*md.BadTimestamp)
		if err != nil {
			return nil, errors.Wrapf(err, "failed parsing badTimestamp for replica '%s', volume '%s'", svc.Name, volumeName)
		}
		return &ts, nil
	}

	return nil, nil
}

func (orc *cattleOrc) getReplicas(volumeName string, stack *client.Stack) (map[string]*types.ReplicaInfo, error) {
	svc, err := orc.getService(volumeName, util.ReplicaServiceName)
	if err != nil {
		return nil, errors.Wrap(err, "error listing replica services")
	}

	replicas := map[string]*types.ReplicaInfo{}
	cntColl := new(client.ContainerCollection)
	if err := orc.rancher.GetLink(svc.Resource, "instances", cntColl); err != nil {
		return nil, errors.Wrapf(err, "error getting replica containers, volume '%s'", volumeName)
	}

	for _, cnt := range cntColl.Data {
		ts, err := orc.getReplicaBadTS(cnt.Name, volumeName, svc)
		if err != nil {
			return nil, err
		}
		hostID, err := orc.getHostID(&cnt)
		if err != nil {
			return nil, err
		}
		replicas[replicaIndex(cnt.Name)] = &types.ReplicaInfo{
			InstanceInfo: types.InstanceInfo{
				ID:      cnt.Id,
				Running: cnt.State == "running",
				HostID:  hostID,
				Address: util.ReplicaAddress(cnt.Name, volumeName),
			},
			Name:         cnt.Name,
			BadTimestamp: ts,
		}
	}

	return replicas, nil
}

func (orc *cattleOrc) getHostID(cnt *client.Container) (string, error) {
	if cnt.HostId == "" {
		return "", nil
	}
	host, err := orc.rancher.Host.ById(cnt.HostId)
	if err != nil {
		return "", errors.Wrapf(err, "error getting host for container '%s', id='%s'", cnt.Name, cnt.Id)
	}
	return host.Uuid, nil
}

func (orc *cattleOrc) getController(volumeName string, stack *client.Stack) (*types.ControllerInfo, error) {
	svc, err := orc.getService(volumeName, util.ControllerServiceName)
	if err != nil {
		return nil, errors.Wrapf(err, "error finding controller, volume '%s'", volumeName)
	}
	cntColl := new(client.ContainerCollection)
	if err := orc.rancher.GetLink(svc.Resource, "instances", cntColl); err != nil {
		return nil, errors.Wrapf(err, "error getting controller container, volume '%s'", volumeName)
	}
	if len(cntColl.Data) > 1 {
		return nil, errors.Errorf("More than 1 controller for volume '%s'", volumeName)
	}

	for _, cnt := range cntColl.Data {
		hostID, err := orc.getHostID(&cnt)
		if err != nil {
			return nil, err
		}
		return &types.ControllerInfo{
			InstanceInfo: types.InstanceInfo{
				ID:      cnt.Id,
				Running: cnt.State == "running",
				HostID:  hostID,
				Address: util.ControllerAddress(volumeName),
			},
		}, nil
	}
	return nil, nil
}

func (orc *cattleOrc) getVolume(volumeName string, stack *client.Stack) (*types.VolumeInfo, error) {
	md, err := orc.getService(volumeName, util.ControllerServiceName)
	if err != nil {
		return nil, errors.Wrapf(err, "error metadata service, volume '%s'", volumeName)
	}
	volume := new(types.VolumeInfo)
	if err := mapstructure.Decode(md.Metadata[volumeProperty], volume); err != nil {
		return nil, errors.Wrapf(err, "Failed to decode metadata for volume '%s'", volumeName)
	}
	if volume.Name != volumeName {
		return nil, errors.Errorf("Name check failed: decoding volume metadata: expected '%s', got '%s'", volumeName, volume.Name)
	}
	volume.StaleReplicaTimeout = volume.StaleReplicaTimeout * time.Hour

	replicas, err := orc.getReplicas(volumeName, stack)
	if err != nil {
		return nil, err
	}

	controller, err := orc.getController(volumeName, stack)
	if err != nil {
		return nil, err
	}

	volume.Replicas = replicas
	volume.Controller = controller

	return volume, nil
}

func (orc *cattleOrc) GetVolume(volumeName string) (*types.VolumeInfo, error) {
	stack, err := orc.getStack(volumeName)
	if err != nil {
		return nil, err
	}
	if stack == nil {
		return nil, nil
	}
	return orc.getVolume(volumeName, stack)
}

func (orc *cattleOrc) getService(volumeName, serviceName string) (*client.Service, error) {
	stack, err := orc.getStack(volumeName)
	if err != nil {
		return nil, err
	}
	svcColl, err := orc.rancher.Service.List(&client.ListOpts{Filters: map[string]interface{}{
		"stackId": stack.Id,
		"name":    serviceName,
	}})
	if err != nil {
		return nil, errors.Wrapf(err, "error listing service '%s'", serviceName)
	}
	if len(svcColl.Data) < 1 {
		return nil, errors.Errorf("Could not find service '%s', volume '%s'", serviceName, volumeName)
	}
	return &svcColl.Data[0], nil
}

func (orc *cattleOrc) MarkBadReplica(volumeName string, replica *types.ReplicaInfo) error {
	orc.Lock()
	defer orc.Unlock()

	svc, err := orc.getService(volumeName, util.ReplicaServiceName)
	if err != nil {
		return err
	}
	replicaName := util.ReplicaName(replica.Address, volumeName)

	ts := util.FormatTimeZ(time.Now().UTC())
	svc.Metadata[replicaName] = replicaMd{BadTimestamp: &ts}
	_, err = orc.rancher.Service.Update(svc, map[string]interface{}{
		"metadata": svc.Metadata,
	})

	return errors.Wrapf(err, "error updating metadata for replica '%s', volume '%s'", replicaName, volumeName)
}

func (orc *cattleOrc) unMarkBadReplica(volumeName string, replica *types.ReplicaInfo, stack *client.Stack) error {
	orc.Lock()
	defer orc.Unlock()

	svc, err := orc.getService(volumeName, util.ReplicaServiceName)
	if err != nil {
		return err
	}

	delete(svc.Metadata, replica.Name)
	_, err = orc.rancher.Service.Update(svc, map[string]interface{}{
		"metadata": svc.Metadata,
	})

	return errors.Wrapf(err, "error updating metadata for replica '%s', volume '%s'", replica.Name, volumeName)
}

func (orc *cattleOrc) CreateController(volumeName string, replicas map[string]*types.ReplicaInfo) (*types.ControllerInfo, error) {
	stack, err := orc.getStack(volumeName)
	if err != nil {
		return nil, err
	}
	if stack == nil {
		return nil, errors.Errorf("can not create controller for non-existent volume '%s'", volumeName)
	}
	volume, err := orc.getVolume(volumeName, stack)
	if err != nil {
		return nil, err
	}
	for _, replica := range replicas {
		if replica.BadTimestamp != nil {
			if err := orc.unMarkBadReplica(volumeName, replica, stack); err != nil {
				return nil, errors.Wrapf(err, "error unmarking bad replica '%s', volume '%s'", replica.Name, volumeName)
			}
		}
	}
	volume.Replicas = replicas

	cnt, err := orc.rancher.Container.Create(orc.controllerContainer(volume))
	if err != nil {
		return nil, errors.Wrapf(err, "error creating controller container, volume '%s'", volume.Name)
	}
	if err := util.Backoff(30*time.Second, "timed out", func() (bool, error) {
		c, err := orc.rancher.Container.ById(cnt.Id)
		if err != nil {
			return false, errors.Wrapf(err, "error getting controller info, volume '%s'", volume.Name)
		}
		if c.State == "running" {
			return true, nil
		}
		return false, nil
	}); err != nil {
		return nil, errors.Wrap(err, "error waiting for controller")
	}

	controller, err := orc.getController(volumeName, stack)
	if err != nil {
		return nil, err
	}
	if err := orc.dragon.WaitForController(volumeName); err != nil {
		if cnt, err := orc.rancher.Container.ById(controller.ID); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error getting controller for cleanup, volume '%s'", volumeName))
		} else if err := orc.rancher.Container.Delete(cnt); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error deleting controller, volume '%s'", volumeName))
		}
		return nil, errors.Wrapf(err, "error waiting for controller, volume '%s'", volumeName)
	}

	return controller, nil
}

func withStartOnCreate(cnt *client.Container) *client.Container {
	cnt.StartOnCreate = true
	return cnt
}

func (orc *cattleOrc) CreateReplica(volumeName string) (*types.ReplicaInfo, error) {
	stack, err := orc.getStack(volumeName)
	if err != nil {
		return nil, err
	}
	if stack == nil {
		return nil, errors.Errorf("can not create replica for non-existent volume '%s'", volumeName)
	}
	volume, err := orc.getVolume(volumeName, stack)
	if err != nil {
		return nil, err
	}
	index := randStr()
	cnt, err := orc.rancher.Container.Create(
		withStartOnCreate(orc.replicaContainer(volume, &types.ReplicaInfo{Name: replicaName(index)})),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating replica container, volume '%s'", volume.Name)
	}
	if err := util.Backoff(30*time.Second, "timed out", func() (bool, error) {
		c, err := orc.rancher.Container.ById(cnt.Id)
		if err != nil {
			return false, errors.Wrapf(err, "error getting replica info, volume '%s'", volume.Name)
		}
		if c.State == "running" {
			return true, nil
		}
		return false, nil
	}); err != nil {
		return nil, errors.Wrap(err, "error waiting for controller")
	}

	replicas, err := orc.getReplicas(volumeName, stack)
	if err != nil {
		return nil, err
	}
	replica := replicas[index]

	if err := orc.dragon.WaitForReplica(volumeName, replica.Name); err != nil {
		if cnt, err := orc.rancher.Container.ById(replica.ID); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error getting replica '%s' for cleanup, volume '%s'", replica.Name, volumeName))
		} else if err := orc.rancher.Container.Delete(cnt); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error deleting replica '%s', volume '%s'", replica.Name, volumeName))
		}
		return nil, errors.Wrapf(err, "error waiting for replica '%s', volume '%s'", replica.Name, volumeName)
	}

	return replica, nil
}

func (orc *cattleOrc) StartInstance(instanceID string) error {
	var cnt *client.Container
	if err := util.Backoff(30*time.Second, "timed out starting", func() (bool, error) {
		var err error
		cnt, err = orc.rancher.Container.ById(instanceID)
		if err != nil {
			return false, errors.Wrapf(err, "error getting container '%s'", instanceID)
		}
		if cnt.State == "running" {
			return true, nil
		}
		if cnt.State == "stopped" {
			_, err = orc.rancher.Container.ActionStart(cnt)
		}
		return false, errors.Wrapf(err, "error starting container '%s'", instanceID)
	}); err != nil {
		return errors.Wrapf(err, "error waiting to start container '%s'", instanceID)
	}

	if volumeName, ok := cnt.Labels["io.rancher.longhorn.replica.volume"].(string); ok {
		err := orc.dragon.WaitForReplica(volumeName, cnt.Name)
		return errors.Wrapf(err, "error waiting to start replica '%s', volume '%s'", cnt.Name, volumeName)
	}
	return nil
}

func (orc *cattleOrc) StopInstance(instanceID string) error {
	var cnt *client.Container
	err := util.Backoff(30*time.Second, "timed out stopping", func() (bool, error) {
		var err error
		cnt, err = orc.rancher.Container.ById(instanceID)
		if err != nil {
			return false, errors.Wrapf(err, "error getting container '%s'", instanceID)
		}
		if cnt.State == "stopped" {
			return true, nil
		}
		if cnt.State == "running" {
			_, err = orc.rancher.Container.ActionStop(cnt, &client.InstanceStop{Timeout: 10})
		}
		return false, errors.Wrapf(err, "error stopping container '%s'", instanceID)
	})
	return errors.Wrapf(err, "error waiting to stop container '%s'", instanceID)
}

func (orc *cattleOrc) RemoveInstance(instanceID string) error {
	cnt, err := orc.rancher.Container.ById(instanceID)
	if err != nil {
		return errors.Wrapf(err, "error getting container '%s'", instanceID)
	}
	err = orc.rancher.Container.Delete(cnt)
	return errors.Wrapf(err, "error deleting service '%s'", cnt.Name)
}

func (orc *cattleOrc) GetCurrentHostID() string {
	return orc.hostUUID
}

func (orc *cattleOrc) ListHosts() (map[string]*types.HostInfo, error) {
	return nil, errors.Errorf("Haven't implemented ListHosts yet")
}

func (orc *cattleOrc) GetHost(id string) (*types.HostInfo, error) {
	return nil, errors.Errorf("Haven't implemented GetHost yet")
}
