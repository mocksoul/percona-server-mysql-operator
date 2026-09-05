#!/bin/bash

set -e

if [ "${CLUSTER_TYPE}" == "async" ]; then
	exit 0
fi

LIB_PATH='/opt/percona/lib'
# shellcheck source=build/lib/util.sh
. ${LIB_PATH}/util.sh

LOG_FILE=/var/lib/mysql/pre-stop.log
NAMESPACE=$(</var/run/secrets/kubernetes.io/serviceaccount/namespace)
OPERATOR_PASSWORD=$(</etc/mysql/mysql-users-secret/operator)
FQDN="${HOSTNAME}.${SERVICE_NAME}.${NAMESPACE}"
# Same address mysqld bound its admin port to in ps-entrypoint.sh.
POD_IP=$(pod_ip)
SERVER_NUM="${HOSTNAME##*-}"

if [[ ${SERVER_NUM} == "0" ]]; then
	echo "$(date +%Y-%m-%dT%H:%M:%S%Z): Not removing ${FQDN} from cluster, it's pod zero" >>${LOG_FILE}
	exit 0
fi

echo "$(date +%Y-%m-%dT%H:%M:%S%Z): Removing ${FQDN} from cluster" >>${LOG_FILE}

mysqlsh --js --no-wizard \
	-h "${POD_IP}" -P 33062 \
	-u operator -p"${OPERATOR_PASSWORD}" \
	-e "dba.getCluster().removeInstance('${FQDN}:3306')" >>${LOG_FILE} 2>&1
