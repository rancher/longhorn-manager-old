package scheduler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/api"
	"github.com/rancher/longhorn-manager/types"
)

type schedulerClient struct {
	hostID  string
	address string
}

func newSchedulerClient(host *types.HostInfo) *schedulerClient {
	address := "http://" + host.Address + "/v1"
	return &schedulerClient{
		hostID:  host.UUID,
		address: address,
	}
}

func (c *schedulerClient) Schedule(item *types.ScheduleItem) (*types.InstanceInfo, error) {
	var output api.ScheduleOutput

	input := &api.ScheduleInput{
		Spec: types.ScheduleSpec{
			HostID: c.hostID,
		},
		Item: *item,
	}
	if err := c.post("/schedule", input, &output); err != nil {
		return nil, errors.Wrap(err, "schedule failure")
	}
	if output.Instance.ID == "" {
		return nil, errors.Errorf("Invalid response with empty instance ID")
	}
	return &output.Instance, nil
}

func (c *schedulerClient) post(path string, req, resp interface{}) error {
	return c.do("POST", path, req, resp)
}

func (c *schedulerClient) do(method, path string, req, resp interface{}) error {
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}

	bodyType := "application/json"
	url := c.address + path

	logrus.Debugf("%s %s", method, url)
	httpReq, err := http.NewRequest(method, url, bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", bodyType)

	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode >= 300 {
		content, _ := ioutil.ReadAll(httpResp.Body)
		return fmt.Errorf("Bad response: %d %s: %s", httpResp.StatusCode, httpResp.Status, content)
	}

	if resp == nil {
		return nil
	}

	return json.NewDecoder(httpResp.Body).Decode(resp)
}
