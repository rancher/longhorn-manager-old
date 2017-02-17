package util

import (
	"errors"
	"fmt"
	"strconv"
	"time"
	"github.com/docker/go-units"
	"crypto/md5"
	"strings"
)

const (
	VolumeStackPrefix = "volume-"
	ControllerName    = "controller"
)

type MetadataConfig struct {
	DriverName          string
	Image               string
	OrcImage            string
	DriverContainerName string
}

func VolumeStackName(volumeName string) string {
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

func ControllerAddress(volumeName string) string {
	return fmt.Sprintf("http://%s.%s.rancher.internal:9501", ControllerName, VolumeStackName(volumeName))
}

func ReplicaAddress(name, volumeName string) string {
	return fmt.Sprintf("tcp://%s.%s:9502", name, VolumeStackName(volumeName))
}

func ReplicaName(address, volumeName string) string {
	s := strings.TrimSuffix(strings.TrimPrefix(address, "tcp://"), ":9502")
	return strings.TrimSuffix(s, "."+VolumeStackName(volumeName))
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
