package cattle

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/docker/distribution/uuid"
	"github.com/docker/libcompose/cli/logger"
	"github.com/docker/libcompose/config"
	"github.com/docker/libcompose/project"
	"github.com/docker/libcompose/project/options"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher-metadata/metadata"
	client "github.com/rancher/go-rancher/v2"
	"github.com/rancher/longhorn-orc/orch"
	"github.com/rancher/longhorn-orc/types"
	"github.com/rancher/longhorn-orc/util"
	"github.com/rancher/rancher-compose/lookup"
	"github.com/rancher/rancher-compose/rancher"
	"github.com/urfave/cli"
	"golang.org/x/net/context"
	"net/http"
	"strings"
	"text/template"
	"time"
)

const (
	volmdName            = "volmd"
	replicaNamePrefix    = "replica"
	badTimestampProperty = "badTimestamp"
	volumeProperty       = "volume"
)

var (
	dockerComposeTemplate  *template.Template
	rancherComposeTemplate *template.Template

	optsUp = options.Up{Create: options.Create{NoRecreate: true}}

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
	rancher  *client.RancherClient
	metadata metadata.Client
	dragon   *hiddenDragon

	hostUUID, containerUUID string

	LonghornImage string

	Env map[string]interface{}
}

func New(c *cli.Context) types.Orchestrator {
	clientOpts := &client.ClientOpts{
		Url:       c.GlobalString("cattle-url"),
		AccessKey: c.GlobalString("cattle-access-key"),
		SecretKey: c.GlobalString("cattle-secret-key"),
	}
	rancherClient, err := client.NewRancherClient(clientOpts)
	if err != nil {
		logrus.Fatalf("%+v", errors.Wrap(err, "failed to get rancher client"))
	}
	logrus.Debugf("rancher clientOpts: %+v", *clientOpts)

	md := metadata.NewClient(c.GlobalString("metadata-url"))
	host, err := md.GetSelfHost()
	if err != nil {
		logrus.Fatalf("%+v", errors.Wrap(err, "failed to get self host from rancher metadata"))
	}
	logrus.Infof("cattle orc: this host UUID: '%s'", host.UUID)

	container, err := md.GetSelfContainer()
	if err != nil {
		logrus.Fatalf("%+v", errors.Wrap(err, "failed to get self container from rancher metadata"))
	}
	logrus.Infof("cattle orc: this container UUID: '%s'", container.UUID)

	return initOrc(&cattleOrc{
		rancher:       rancherClient,
		metadata:      md,
		dragon:        &hiddenDragon{httpClient: &http.Client{Timeout: 10 * time.Second}},
		hostUUID:      host.UUID,
		containerUUID: container.UUID,
		LonghornImage: c.GlobalString(orch.LonghornImageParam),
	})
}

func initOrc(orc *cattleOrc) *cattleOrc {
	orc.Env = map[string]interface{}{
		"LONGHORN_IMAGE": orc.LonghornImage,
		"ORC_CONTAINER":  orc.containerUUID,
	}
	logrus.Infof("volume stack env: %+v", orc.Env)
	return orc
}

func volumeStackExternalID(name string) string {
	return fmt.Sprintf("system://%s?name=%s", "rancher-longhorn", name)
}

func replicaName(index string) string {
	return "replica-" + index
}

func stackBytes(t *template.Template, volume *types.VolumeInfo) []byte {
	buffer := new(bytes.Buffer)
	if err := t.Execute(buffer, volume); err != nil {
		logrus.Fatalf("Error applying the stack golang template: %v", err)
	}
	logrus.Debugf("%s", buffer)
	return buffer.Bytes()
}

func (orc *cattleOrc) envLookup() config.EnvironmentLookup {
	return &lookup.MapEnvLookup{Env: orc.Env}
}

func (orc *cattleOrc) composeProject(volume *types.VolumeInfo, stack *client.Stack) project.APIProject {
	ctx := &rancher.Context{
		Context: project.Context{
			EnvironmentLookup: orc.envLookup(),
			LoggerFactory:     logger.NewColorLoggerFactory(),
			ComposeBytes:      [][]byte{stackBytes(dockerComposeTemplate, volume)},
		},
		RancherComposeBytes: stackBytes(rancherComposeTemplate, volume),
		Client:              orc.rancher,
		Stack:               stack,
	}
	p, err := rancher.NewProject(ctx)
	if err != nil {
		logrus.Fatalf("%+v", errors.Wrap(err, "error creating compose project"))
	}
	p.Name = util.VolumeStackName(volume.Name)
	return p
}

func copyVolumeProperties(volume0 *types.VolumeInfo) *types.VolumeInfo {
	volume := new(types.VolumeInfo)
	*volume = *volume0
	volume.Controller = nil
	volume.Replicas = nil
	volume.State = nil
	return volume
}

func randStr() string {
	buf := new(bytes.Buffer)
	b64 := base64.NewEncoder(base64.RawURLEncoding, buf)
	u := uuid.Generate()
	b64.Write(u[:])
	s := buf.String()[:10]
	s = strings.Replace(s, "_", "0", -1)
	return strings.Replace(s, "-", "A", -1)
}

func (orc *cattleOrc) CreateVolume(volume *types.VolumeInfo) (*types.VolumeInfo, error) {
	if v, err := orc.GetVolume(volume.Name); err != nil {
		return nil, err
	} else if v != nil {
		return v, nil
	}
	volume = copyVolumeProperties(volume)
	stack0 := &client.Stack{
		Name:        util.VolumeStackName(volume.Name),
		ExternalId:  volumeStackExternalID(volume.Name),
		Environment: orc.Env,
	}
	stack, err := orc.rancher.Stack.Create(stack0)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create stack '%s'", stack0.Name)
	}

	replicas := map[string]*types.ReplicaInfo{}
	replicaNames := make([]string, volume.NumberOfReplicas)
	for i := 0; i < volume.NumberOfReplicas; i++ {
		index := randStr()
		name := replicaName(index)
		replicas[index] = &types.ReplicaInfo{Name: name}
		replicaNames[i] = name
	}
	volume.Replicas = replicas

	p := orc.composeProject(volume, stack)
	if err := p.Create(context.Background(), optsUp.Create, append(replicaNames, volmdName)...); err != nil {
		return nil, errors.Wrap(err, "failed to create volume stack services")
	}

	return orc.GetVolume(volume.Name)
}

func (orc *cattleOrc) DeleteVolume(volumeName string) error {
	stack, err := orc.getStack(volumeName)
	if err != nil {
		return err
	}
	if stack == nil {
		return nil
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

func (orc *cattleOrc) getReplicas(volumeName string, stack *client.Stack) (map[string]*types.ReplicaInfo, error) {
	svcColl, err := orc.rancher.Service.List(&client.ListOpts{Filters: map[string]interface{}{
		"stackId":     stack.Id,
		"name_prefix": replicaNamePrefix,
	}})
	if err != nil {
		return nil, errors.Wrap(err, "error listing replica services")
	}

	replicas := map[string]*types.ReplicaInfo{}
	for _, svc := range svcColl.Data {
		index := svc.Name[len(replicaNamePrefix)+1:]
		replica := &types.ReplicaInfo{
			InstanceInfo: types.InstanceInfo{
				ID:      svc.Id,
				Running: svc.State == "active",
				Address: util.ReplicaAddress(svc.Name, volumeName),
			},
			Name: svc.Name,
		}
		replicas[index] = replica
		if badTS, ok := svc.Metadata[badTimestampProperty].(string); ok {
			ts, err := util.ParseTimeZ(badTS)
			if err != nil {
				return nil, errors.Wrapf(err, "failed parsing badTimestamp for replica '%s', volume '%s'", svc.Name, volumeName)
			}
			replica.BadTimestamp = &ts
		}
	}

	return replicas, nil
}

func (orc *cattleOrc) getController(volumeName string, stack *client.Stack) (*types.ControllerInfo, error) {
	svcColl, err := orc.rancher.Service.List(&client.ListOpts{Filters: map[string]interface{}{
		"stackId": stack.Id,
		"name":    util.ControllerName,
	}})
	if err != nil {
		return nil, errors.Wrap(err, "error finding controller")
	}
	if len(svcColl.Data) > 1 {
		return nil, errors.Errorf("More than 1 controller for volume '%s'", volumeName)
	}
	for _, svc := range svcColl.Data {
		return &types.ControllerInfo{
			InstanceInfo: types.InstanceInfo{
				ID:      svc.Id,
				Running: svc.State == "active",
				Address: util.ControllerAddress(volumeName),
			},
		}, nil
	}
	return nil, nil
}

func (orc *cattleOrc) getVolume(volumeName string, stack *client.Stack) (*types.VolumeInfo, error) {
	svcColl, err := orc.rancher.Service.List(&client.ListOpts{Filters: map[string]interface{}{
		"stackId": stack.Id,
		"name":    volmdName,
	}})
	if err != nil {
		return nil, errors.Wrap(err, "error getting volmd")
	}
	if len(svcColl.Data) != 1 {
		return nil, errors.Errorf("Failed to get metadata for volume '%s'", volumeName)
	}
	md := svcColl.Data[0]
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

func (orc *cattleOrc) MarkBadReplica(volumeName string, replica *types.ReplicaInfo) error {
	stack, err := orc.getStack(volumeName)
	if err != nil {
		return err
	}
	replicaName := util.ReplicaName(replica.Address, volumeName)
	svcColl, err := orc.rancher.Service.List(&client.ListOpts{Filters: map[string]interface{}{
		"stackId": stack.Id,
		"name":    replicaName,
	}})
	if err != nil {
		return errors.Wrapf(err, "error finding replica '%s' for volume '%s'", replicaName, volumeName)
	}
	if len(svcColl.Data) < 1 {
		return errors.Errorf("Could not find replica named '%s' for volume '%s'", replicaName, volumeName)
	}
	if len(svcColl.Data) > 1 {
		return errors.Errorf("More than 1 replica named '%s' for volume '%s'", replicaName, volumeName)
	}
	svc := &svcColl.Data[0]

	svc.Metadata[badTimestampProperty] = util.FormatTimeZ(time.Now().UTC())
	_, err = orc.rancher.Service.Update(svc, map[string]interface{}{
		"metadata": svc.Metadata,
	})

	return errors.Wrapf(err, "error updating metadata")
}

func (orc *cattleOrc) unMarkBadReplica(volumeName string, replica *types.ReplicaInfo, stack *client.Stack) error {
	svcColl, err := orc.rancher.Service.List(&client.ListOpts{Filters: map[string]interface{}{
		"stackId": stack.Id,
		"name":    replica.Name,
	}})
	if err != nil {
		return errors.Wrapf(err, "error finding replica '%s' for volume '%s'", replica.Name, volumeName)
	}
	if len(svcColl.Data) < 1 {
		return errors.Errorf("Could not find replica named '%s' for volume '%s'", replica.Name, volumeName)
	}
	if len(svcColl.Data) > 1 {
		return errors.Errorf("More than 1 replica named '%s' for volume '%s'", replica.Name, volumeName)
	}
	svc := &svcColl.Data[0]

	delete(svc.Metadata, badTimestampProperty)
	_, err = orc.rancher.Service.Update(svc, map[string]interface{}{
		"metadata": svc.Metadata,
	})

	return errors.Wrapf(err, "error updating metadata")
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
	volume.Controller = &types.ControllerInfo{}
	p := orc.composeProject(volume, stack)
	if err := p.Up(context.Background(), optsUp, util.ControllerName); err != nil {
		return nil, errors.Wrap(err, "failed to create controller service")
	}
	controller, err := orc.getController(volumeName, stack)
	if err != nil {
		return nil, err
	}
	if err := orc.dragon.WaitForController(volumeName); err != nil {
		if svc, err := orc.rancher.Service.ById(controller.ID); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error getting controller for cleanup, volume '%s'", volumeName))
		} else if err := orc.rancher.Service.Delete(svc); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error deleting controller, volume '%s'", volumeName))
		}
		return nil, errors.Wrapf(err, "error waiting for controller, volume '%s'", volumeName)
	}

	return controller, nil
}

func (orc *cattleOrc) CreateReplica(volumeName string) (*types.ReplicaInfo, error) {
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
	index := randStr()
	replica := &types.ReplicaInfo{Name: replicaName(index)}
	volume.Replicas[index] = replica
	p := orc.composeProject(volume, stack)
	if err := p.Up(context.Background(), optsUp, replica.Name); err != nil {
		return nil, errors.Wrap(err, "failed to create replica service")
	}

	replicas, err := orc.getReplicas(volumeName, stack)
	if err != nil {
		return nil, err
	}

	if err := orc.dragon.WaitForReplica(volumeName, replica.Name); err != nil {
		if svc, err := orc.rancher.Service.ById(replicas[index].ID); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error getting replica '%s' for cleanup, volume '%s'", replica.Name, volumeName))
		} else if err := orc.rancher.Service.Delete(svc); err != nil {
			logrus.Errorf("%+v", errors.Wrapf(err, "error deleting replica '%s', volume '%s'", replica.Name, volumeName))
		}
		return nil, errors.Wrapf(err, "error waiting for replica '%s', volume '%s'", replica.Name, volumeName)
	}

	return replicas[index], nil
}

func (orc *cattleOrc) startSvc(attempts int, svc0 *client.Service, errCh chan<- error) {
	switch svc0.State {
	case "active":
		errCh <- nil
		return
	case "inactive":
		svc, err := orc.rancher.Service.ActionActivate(svc0)
		if err != nil {
			errCh <- errors.Wrapf(err, "error starting service '%s'", svc0.Id)
			return
		}
		go orc.startSvc(attempts+10, svc, errCh)
	default:
		if attempts <= 0 {
			errCh <- errors.Errorf("giving up starting service '%s'", svc0.Name)
			return
		}
		<-time.NewTimer(time.Second).C
		svc, err := orc.rancher.Service.ById(svc0.Id)
		if err != nil {
			errCh <- errors.Wrapf(err, "error getting service '%s'", svc0.Name)
			return
		}
		go orc.startSvc(attempts-1, svc, errCh)
	}
}

func (orc *cattleOrc) StartReplica(instanceID string) error {
	svc, err := orc.rancher.Service.ById(instanceID)
	if err != nil {
		return errors.Wrapf(err, "error getting service '%s'", instanceID)
	}
	errCh := make(chan error)
	defer close(errCh)
	go orc.startSvc(50, svc, errCh)
	if err := <-errCh; err != nil {
		return err
	}
	volumeName := svc.LaunchConfig.Labels["io.rancher.longhorn.replica.volume"].(string)
	err = orc.dragon.WaitForReplica(volumeName, svc.Name)
	return errors.Wrapf(err, "error waiting to start replica")
}

func (orc *cattleOrc) stopSvc(attempts int, svc0 *client.Service, errCh chan<- error) {
	switch svc0.State {
	case "inactive":
		errCh <- nil
		return
	case "active":
		svc, err := orc.rancher.Service.ActionDeactivate(svc0)
		if err != nil {
			errCh <- errors.Wrapf(err, "error stopping service '%s'", svc0.Id)
			return
		}
		go orc.stopSvc(attempts+10, svc, errCh)
	default:
		if attempts <= 0 {
			errCh <- errors.Errorf("giving up stopping service '%s'", svc0.Name)
			return
		}
		<-time.NewTimer(time.Second).C
		svc, err := orc.rancher.Service.ById(svc0.Id)
		if err != nil {
			errCh <- errors.Wrapf(err, "error getting service '%s'", svc0.Name)
			return
		}
		go orc.stopSvc(attempts-1, svc, errCh)
	}
}

func (orc *cattleOrc) StopReplica(instanceID string) error {
	svc, err := orc.rancher.Service.ById(instanceID)
	if err != nil {
		return errors.Wrapf(err, "error getting service '%s'", instanceID)
	}
	errCh := make(chan error)
	defer close(errCh)
	go orc.stopSvc(50, svc, errCh)
	return <-errCh
}

func (orc *cattleOrc) RemoveInstance(instanceID string) error {
	svc, err := orc.rancher.Service.ById(instanceID)
	if err != nil {
		return errors.Wrapf(err, "error getting service '%s'", instanceID)
	}
	err = orc.rancher.Service.Delete(svc)
	return errors.Wrapf(err, "error deleting service '%s'", svc.Name)
}

func (orc *cattleOrc) GetThisHostID() string {
	return orc.hostUUID
}
