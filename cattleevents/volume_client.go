package cattleevents

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/go-rancher/v2"
	"github.com/rancher/longhorn-orc/util"
)

func newVolumeClient(snapshot *eventSnapshot) *volumeClient {
	return newVolumeClientFromName(snapshot.Volume.Name)
}

func newVolumeClientFromName(volumeName string) *volumeClient {
	url := fmt.Sprintf("http://controller.%v.rancher.internal/v1", util.VolumeToStackName(volumeName))
	return &volumeClient{
		baseURL: url,
	}
}

type volumeClient struct {
	baseURL string
}

func (c *volumeClient) reloadStatus(s *status) (*status, error) {
	self, ok := s.Links["self"]
	if !ok {
		return nil, fmt.Errorf("Status doesn't have self link")
	}

	resp, err := http.Get(self)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	stat := &status{}
	err = json.NewDecoder(resp.Body).Decode(stat)
	if err != nil {
		return nil, err
	}

	return stat, nil
}

func (c *volumeClient) revertToSnapshot(name string) (*volume, error) {
	var resp volume
	request := &snapshot{
		Name: name,
	}
	err := c.post("/volumes/1?action=reverttosnapshot", request, &resp)
	return &resp, err
}

func (c *volumeClient) removeBackup(snapshotUUID, uuid, location string, target backupTarget) (*status, error) {
	request := &locationInput{
		UUID:         uuid,
		Location:     location,
		BackupTarget: target,
	}
	if err := c.post(fmt.Sprintf("/snapshots/%v?action=removebackup", snapshotUUID), request, nil); err != nil {
		if apiErr, ok := err.(apiError); ok && apiErr.statusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, err
	}

	return nil, nil
}

func (c *volumeClient) createBackup(snapshotUUID, uuid string, target backupTarget) (*status, error) {
	var resp status
	request := &backupInput{
		UUID:         uuid,
		BackupTarget: target,
	}
	err := c.post(fmt.Sprintf("/snapshots/%v?action=backup", snapshotUUID), request, &resp)
	return &resp, err
}

func (c *volumeClient) restoreFromBackup(uuid, location string, target backupTarget) (*status, error) {
	var resp status
	request := &locationInput{
		UUID:         uuid,
		Location:     location,
		BackupTarget: target,
	}

	err := c.post("/volumes/1?action=restorefrombackup", request, &resp)
	return &resp, err
}

func (c *volumeClient) listSnapshots() ([]snapshot, error) {
	var resp snapshotCollection
	err := c.get("/snapshots", &resp)
	return resp.Data, err
}

func (c *volumeClient) getSnapshot(name string) (*snapshot, error) {
	var resp snapshot
	err := c.get(fmt.Sprintf("/snapshots/%v", name), &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (c *volumeClient) createSnapshot(name string) (*snapshot, error) {
	var resp snapshot
	request := &snapshot{
		Name: name,
	}
	err := c.post("/snapshots", request, &resp)
	return &resp, err
}

func (c *volumeClient) deleteSnapshot(name string) error {
	if err := c.do("DELETE", fmt.Sprintf("/snapshots/%v", name), nil, nil); err != nil {
		if apiErr, ok := err.(apiError); ok && apiErr.statusCode == http.StatusNotFound {
			return nil
		}
		return err
	}

	return nil
}

func (c *volumeClient) get(path string, obj interface{}) error {
	resp, err := http.Get(c.baseURL + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(obj)
}

func (c *volumeClient) post(path string, req, resp interface{}) error {
	return c.do("POST", path, req, resp)
}

func (c *volumeClient) put(path string, req, resp interface{}) error {
	return c.do("PUT", path, req, resp)
}

func (c *volumeClient) do(method, path string, req, resp interface{}) error {
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}

	bodyType := "application/json"
	url := c.baseURL + path

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
		return newAPIError(httpResp)
	}

	if resp == nil {
		return nil
	}

	return json.NewDecoder(httpResp.Body).Decode(resp)
}

func newAPIError(resp *http.Response) apiError {
	content, _ := ioutil.ReadAll(resp.Body)
	msg := fmt.Sprintf("Bad response: %d %s: %s", resp.StatusCode, resp.Status, content)
	return apiError{
		statusCode: resp.StatusCode,
		status:     resp.Status,
		errorMsg:   msg,
	}
}

type apiError struct {
	statusCode int
	status     string
	errorMsg   string
}

func (e apiError) Error() string {
	return e.errorMsg
}

type volume struct {
	client.Resource
	Name string `json:"name,omitempty"`
}

type backupInput struct {
	UUID         string       `json:"uuid,omitempty"`
	BackupTarget backupTarget `json:"backupTarget,omitempty"`
}

type locationInput struct {
	UUID         string       `json:"uuid,omitempty"`
	Location     string       `json:"location,omitempty"`
	BackupTarget backupTarget `json:"backupTarget,omitempty"`
}

type snapshot struct {
	client.Resource
	Name string `json:"name,omitempty"`
}

type snapshotCollection struct {
	client.Collection
	Data []snapshot `json:"data"`
}

type status struct {
	client.Resource
	State   string `json:"state,omitempty"`
	Message string `json:"message,omitempty"`
}

type backupTarget struct {
	Name      string    `json:"name,omitempty"`
	UUID      string    `json:"uuid,omitempty"`
	NFSConfig nfsConfig `json:"nfsConfig,omitempty"`
}

type nfsConfig struct {
	Server       string `json:"server"`
	Share        string `json:"share"`
	MountOptions string `json:"mountOptions"`
}
