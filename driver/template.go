package driver

const (
	DockerComposeTemplate = `
replica:
    scale: 2
    image: {{if .ReplicaBaseImage}}{{.ReplicaBaseImage}}{{else}}$IMAGE{{end}}
    entrypoint:
    {{if .ReplicaBaseImage -}}
    - /cmd/launch-with-vm-backing-file
    {{else -}}
    - longhorn
    {{end -}}
    command:
    - replica
    - --listen
    - 0.0.0.0:9502
    - --sync-agent=false
    - /volume/$VOLUME_NAME
    volumes:
    - /volume/$VOLUME_NAME
    - /var/lib/rancher/longhorn/backups:/var/lib/rancher/longhorn/backups   #TODO :shared
    {{ if .ReplicaBaseImage -}}
    volumes_from:
    - replica-binary
    {{end -}}
    {{- if (ne .ReadIOPS "") -}}
    device_read_iops:
        DEFAULT_DISK: {{.ReadIOPS}}
    {{end -}}
    {{- if (ne .WriteIOPS "") -}}
    device_write_iops:
        DEFAULT_DISK: {{.WriteIOPS}}
    {{end -}}
    labels:
        io.rancher.sidekicks: replica-api, sync-agent{{if .ReplicaBaseImage}}, replica-binary{{end}}
        io.rancher.container.hostname_override: container_name
        io.rancher.scheduler.affinity:container_label_ne: io.rancher.stack_service.name=$${stack_name}/$${service_name}
        io.rancher.scheduler.affinity:container_soft: $DRIVER_CONTAINER
        {{- if (and (eq .ReplicaBaseImage "") (ne .SizeGB "0") (ne .SizeGB "")) }}
        io.rancher.resource.disksize.{{.Name}}: {{.SizeGB}}
        {{- end}}
        {{- if (and (eq .ReplicaBaseImage "") (ne .ReadIOPS "")) }}
        io.rancher.resource.read_iops.{{.Name}}: {{.ReadIOPS}}
        {{- end}}
        {{- if (and (eq .ReplicaBaseImage "") (ne .WriteIOPS "")) }}
        io.rancher.resource.write_iops.{{.Name}}: {{.WriteIOPS}}
        {{- end}}
    metadata:
        volume:
            volume_name: $VOLUME_NAME
            volume_size: $VOLUME_SIZE
    health_check:
        healthy_threshold: 1
        unhealthy_threshold: 4
        interval: 5000
        port: 8199
        request_line: GET /replica/status HTTP/1.0
        response_timeout: 50000
        strategy: recreateOnQuorum
        recreate_on_quorum_strategy_config:
            quorum: 1

{{- if .ReplicaBaseImage}}

replica-binary:
    image: $IMAGE
    net: none
    command: copy-binary
    volumes:
    - /cmd
    labels:
        io.rancher.container.start_once: true
        {{- if (and (ne .SizeGB "0") (ne .SizeGB "")) }}
        io.rancher.resource.disksize.{{.Name}}: {{.SizeGB}}
        {{- end}}
        {{- if (ne .ReadIOPS "") }}
        io.rancher.resource.read_iops.{{.Name}}: {{.ReadIOPS}}
        {{- end}}
        {{- if (ne .WriteIOPS "") }}
        io.rancher.resource.write_iops.{{.Name}}: {{.WriteIOPS}}
        {{- end}}
{{- end}}

sync-agent:
    image: {{if .ReplicaBaseImage}}{{.ReplicaBaseImage}}{{else}}$IMAGE{{end}}
    entrypoint:
    {{if .ReplicaBaseImage -}}
    - /cmd/launch-with-vm-backing-file
    {{else -}}
    - longhorn
    {{end -}}
    net: container:replica
    working_dir: /volume/$VOLUME_NAME
    volumes_from:
    - replica
    command:
    - sync-agent
    - --listen
    - 0.0.0.0:9504

replica-api:
    image: $ORC_IMAGE
    privileged: true
    pid: host
    net: container:replica
    volumes_from:
    - replica
    metadata:
        volume:
            volume_name: $VOLUME_NAME
            volume_size: $VOLUME_SIZE
    command:
    - longhorn-agent
    - --replica

controller:
    image: $IMAGE
    command:
    - launch
    - controller
    - --listen
    - 0.0.0.0:9501
    - --frontend
    - tgt
    - $VOLUME_NAME
    privileged: true
    volumes:
    - /dev:/host/dev
    - /proc:/host/proc
    labels:
        io.rancher.sidekicks: controller-agent, lhcmd
        io.rancher.container.hostname_override: container_name
        io.rancher.scheduler.affinity:container: $DRIVER_CONTAINER
    metadata:
        volume:
          volume_name: $VOLUME_NAME
          volume_config: {{.JSON}}
    health_check:
        healthy_threshold: 1
        unhealthy_threshold: 2
        interval: 5000
        port: 8199
        request_line: GET /controller/status HTTP/1.0
        response_timeout: 5000
        strategy: none

controller-agent:
    image: $ORC_IMAGE
    net: container:controller
    metadata:
        volume:
          volume_name: $VOLUME_NAME
    volumes_from: [lhcmd]
    command:
    - longhorn-agent
    - --controller

lhcmd:
    image: $IMAGE
    command: [sh]
    stdin_open: true
    tty: true
`
)
