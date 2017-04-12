package cattle

import (
	"strconv"

	"github.com/pkg/errors"
	"github.com/rancher/longhorn-manager/api"
)

func (orc *cattleOrc) GetAddress(hostID string) (string, error) {
	svc, err := orc.metadata.GetSelfService()
	if err != nil {
		return "", errors.Wrap(err, "error getting self/service from metadata")
	}
	for _, c := range svc.Containers {
		if c.HostUUID == hostID {
			// FIXME Port should be a part of address
			return c.Name + ":" + strconv.Itoa(api.DefaultPort), nil
		}
	}
	return "", nil
}
