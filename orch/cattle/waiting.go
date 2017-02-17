package cattle

import (
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-orc/util"
	"net/http"
	"time"
)

type hiddenDragon struct {
	httpClient *http.Client
}

func (w *hiddenDragon) waitForOK(attempts int, url string, errCh chan<- error) {
	resp, err := w.httpClient.Get(url)
	if err != nil {
		logrus.Debugf("%v", errors.Wrapf(err, "error getting '%s'", url))
		<-time.NewTimer(time.Second).C
		go w.waitForOK(attempts-1, url, errCh)
		return
	}
	resp.Body.Close()

	if 200 <= resp.StatusCode && resp.StatusCode < 300 {
		logrus.Infof("Got OK from '%s'", url)
		errCh <- nil
		return
	}
	if attempts <= 0 {
		errCh <- errors.Errorf("giving up getting '%s'", url)
		return
	}

	<-time.NewTimer(time.Second).C
	go w.waitForOK(attempts-1, url, errCh)
}

func (w *hiddenDragon) WaitForController(volumeName string) error {
	if w == nil {
		return nil
	}
	errCh := make(chan error)
	defer close(errCh)
	go w.waitForOK(30, "http://controller."+util.VolumeStackName(volumeName)+":9501/v1/replicas", errCh)
	return <-errCh
}

func (w *hiddenDragon) WaitForReplica(volumeName, replicaName string) error {
	if w == nil {
		return nil
	}
	errCh := make(chan error)
	defer close(errCh)
	go w.waitForOK(30, "http://"+replicaName+"."+util.VolumeStackName(volumeName)+":9502/v1", errCh)
	return <-errCh
}
