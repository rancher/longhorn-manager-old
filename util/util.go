package util

import (
	"crypto/md5"
	"fmt"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

const (
	VolumeStackPrefix     = "volume-"
	ControllerServiceName = "controller"
	ReplicaServiceName    = "replica"
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
	return fmt.Sprintf("http://%s.%s.rancher.internal:9501", ControllerServiceName, VolumeStackName(volumeName))
}

func ReplicaAddress(name, volumeName string) string {
	return fmt.Sprintf("tcp://%s.rancher.internal:9502", name)
}

func ReplicaName(address, volumeName string) string {
	s := strings.TrimSuffix(strings.TrimPrefix(address, "tcp://"), ":9502")
	s = strings.TrimSuffix(s, ".rancher.internal")
	return strings.TrimSuffix(s, "."+VolumeStackName(volumeName))
}

func ConvertSize(size interface{}) (int64, error) {
	switch size := size.(type) {
	case int64:
		return size, nil
	case int:
		return int64(size), nil
	case string:
		if size == "" {
			return 0, nil
		}
		sizeInBytes, err := units.RAMInBytes(size)
		if err != nil {
			return 0, errors.Wrapf(err, "error parsing size '%s'", size)
		}
		return sizeInBytes, nil
	}
	return 0, errors.Errorf("could not parse size '%v'", size)
}

func RoundUpSize(size int64) int64 {
	if size <= 0 {
		return 4096
	}
	r := size % 4096
	if r == 0 {
		return size
	}
	return size - r + 4096
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

func UUID() string {
	return uuid.NewV4().String()
}
