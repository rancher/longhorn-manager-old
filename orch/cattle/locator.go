package cattle

import (
	"github.com/pkg/errors"
)

func (orc *cattleOrc) GetAddress(hostID string) (string, error) {
	svc, err := orc.metadata.GetSelfService()
	if err != nil {
		return "", errors.Wrap(err, "error getting self/service from metadata")
	}
	for _, c := range svc.Containers {
		if c.HostUUID == hostID {
			return c.Name, nil
		}
	}
	return "", nil
}
