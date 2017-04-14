#!/bin/bash

set -e

source ./common.sh

network=$1
longhorn_manager_ip=$2

if [ "$network" == "" -o "$longhorn_manager_ip" == "" ]; then
        echo usage: $(basename $0) \<network\> \<longhorn_manager_ip\>
        exit -1
fi

host_port=8080

LONGHORN_UI_NAME="longhorn-ui"
LONGHORN_UI_IMAGE="rancher/longhorn-ui:5528110"

cleanup $LONGHORN_UI_NAME
docker run -d \
        --name ${LONGHORN_UI_NAME} \
        --network ${network} \
        -p ${host_port}:8000/tcp \
        -e LONGHORN_MANAGER_IP=http://${longhorn_manager_ip}:9500 \
        ${LONGHORN_UI_IMAGE}

echo Longhorn UI is up at port ${host_port}
