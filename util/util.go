package util

import (
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"time"

	"github.com/docker/go-units"

	"crypto/md5"
	"github.com/rancher/go-rancher-metadata/metadata"
	"strings"
)

const (
	DevDir            = "/dev/longhorn"
	VolumeStackPrefix = "volume-"
)

var (
	cmdTimeout = time.Minute // one minute by default
)

type MetadataConfig struct {
	DriverName          string
	Image               string
	OrcImage            string
	DriverContainerName string
}

func GetMetadataConfig(metadataURL string) (MetadataConfig, error) {
	config := MetadataConfig{}
	client, err := metadata.NewClientAndWait(metadataURL)
	if err != nil {
		return config, err
	}

	config.DriverName = "rancher-longhorn" // TODO get from <svc>.storage_driver.name in rancher-compose

	svc, err := client.GetSelfService()
	if err != nil {
		return config, err
	}
	if image, ok := svc.Metadata["VOLUME_STACK_IMAGE"]; ok {
		config.Image = fmt.Sprintf("%v", image)
	}
	if image, ok := svc.Metadata["ORC_IMAGE"]; ok { // TODO get this container image
		config.OrcImage = fmt.Sprintf("%v", image)
	}

	c, err := client.GetSelfContainer()
	if err != nil {
		return config, err
	}
	config.DriverContainerName = c.UUID

	return config, nil
}

func ConstructSocketNameInContainer(driverName string) string {
	return fmt.Sprintf("/host/var/run/%v.sock", driverName)
}

func ConstructSocketNameOnHost(driverName string) string {
	return fmt.Sprintf("/var/run/%v.sock", driverName)
}

func Execute(binary string, args []string) (string, error) {
	var output []byte
	var err error
	cmd := exec.Command(binary, args...)
	done := make(chan struct{})

	go func() {
		output, err = cmd.CombinedOutput()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(cmdTimeout):
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		return "", fmt.Errorf("Timeout executing: %v %v, output %v, error %v", binary, args, string(output), err)
	}

	if err != nil {
		return "", fmt.Errorf("Failed to execute: %v %v, output %v, error %v", binary, args, string(output), err)
	}
	return string(output), nil
}

func ConvertSize(size string) (string, string, error) {
	if size == "" {
		return "", "", nil
	}

	sizeInBytes, err := units.RAMInBytes(size)
	if err != nil {
		return "", "", err
	}

	gbSize := sizeInBytes / units.GiB
	if gbSize < 1 && sizeInBytes != 0 {
		gbSize = 1
	}
	return strconv.FormatInt(sizeInBytes, 10), strconv.FormatInt(gbSize, 10), nil

}

func VolumeToStackName(volumeName string) string {
	nameNoUnderscores := strings.Replace(volumeName, "_", "-", -1)
	stackName := VolumeStackPrefix + nameNoUnderscores
	if len(stackName) > 63 {
		hash := fmt.Sprintf("%x", md5.Sum([]byte(nameNoUnderscores)))
		leftover := 63 - (len(VolumeStackPrefix) + len(hash) + 1)
		partialName := nameNoUnderscores[0:leftover]
		stackName = VolumeStackPrefix + partialName + "-" + hash
	}
	return stackName
}

func Backoff(maxDuration time.Duration, timeoutMessage string, f func() (bool, error)) error {
	startTime := time.Now()
	waitTime := 150 * time.Millisecond
	maxWaitTime := 2 * time.Second
	for {
		if time.Now().Sub(startTime) > maxDuration {
			return errors.New(timeoutMessage)
		}

		if done, err := f(); err != nil {
			return err
		} else if done {
			return nil
		}

		time.Sleep(waitTime)

		waitTime *= 2
		if waitTime > maxWaitTime {
			waitTime = maxWaitTime
		}
	}
}
