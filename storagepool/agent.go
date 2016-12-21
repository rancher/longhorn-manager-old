package storagepool

import (
	"time"

	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn-orc/cattle"
)

type Agent struct {
	healthCheckInterval int
	driver              string
	cattleClient        cattle.StoragePoolManager
}

func NewStoragepoolAgent(healthCheckInterval int, driver string, cattleClient cattle.StoragePoolManager) *Agent {
	return &Agent{
		healthCheckInterval: healthCheckInterval,
		driver:              driver,
		cattleClient:        cattleClient,
	}
}

func (s *Agent) Run(metadataURL string) error {
	prevSent := map[string]bool{}

	hc, err := newHealthChecker(metadataURL)
	if err != nil {
		log.Errorf("Error initializing health checker, err = [%v]", err)
		return err
	}

	for {
		time.Sleep(time.Duration(s.healthCheckInterval) * time.Millisecond)

		currHosts, err := hc.populateHostMap()
		if err != nil {
			return fmt.Errorf("Error while reading host info [%v]", err)
		}

		toSend := map[string]bool{}
		for uuid := range currHosts {
			toSend[uuid] = true
		}

		shouldSend := false
		for key := range toSend {
			if _, ok := prevSent[key]; !ok {
				shouldSend = true
			}
		}

		for key := range prevSent {
			if _, ok := toSend[key]; !ok {
				shouldSend = true
			}
		}

		if shouldSend {
			toSendList := []string{}
			for k := range toSend {
				toSendList = append(toSendList, k)
			}
			err := s.cattleClient.SyncStoragePool(s.driver, toSendList)
			if err != nil {
				log.Errorf("Error syncing storage pool events [%v]", err)
				return fmt.Errorf("Error syncing storage pool events [%v]", err)
			}
			prevSent = toSend
		}
	}
}
