longhorn-orc
========

Orc is the Longhorn herder.

It's mood is high unstable now.

## Requirement

1. Ubuntu v16.04
2. Docker v1.12

For using Docker orchestrator, make sure you have `iscsiadm`/`open-iscsi` installed on the host.

## Building

`make`

## Running

`./bin/longhorn-orc`

## Example Server

It can be run as a single node example server.

Use `make server` to start the server bind-mounted to host port `9500`. It will contain necessary components for Docker orchestrator to work, e.g. etcd server for k/v store, nfs server for backupstore. Each of them will be started as a container.

The URL for backupstore will show up as: `nfs://xxx.xxx.xxx.xxx:/opt/backupstore` in the console. You can update `backupTarget` accordingly in the `settings`.

Use `make server-cleanup` to cleanup the example servers.

## License
Copyright (c) 2014-2016 [Rancher Labs, Inc.](http://rancher.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
