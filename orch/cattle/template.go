package cattle

const (
	dockerComposeText = `
version: '2'
services:
  replica:
    image: rancher/none-replica
    labels:
      io.rancher.service.selector.container: io.rancher.longhorn.replica.volume={{$.Name}}

  controller:
    image: rancher/none-controller
    labels:
      io.rancher.service.selector.container: io.rancher.longhorn.controller.volume={{$.Name}}
`

	rancherComposeText = `
version: '2'
services:
  controller:
    metadata:
      volume:
        name: {{$.Name}}
        size: {{$.Size}}
        numberOfReplicas: {{$.NumberOfReplicas}}
        staleReplicaTimeout: {{$.StaleReplicaTimeout.Hours}}
`
)
