#!/bin/bash

if [ "${SKIP_EXEC_RC_LOCAL}" = "YES" ] ; then
	echo "skip /etc/rc.local: SKIP_EXEC_RC_LOCAL=${SKIP_EXEC_RC_LOCAL}"
	exit
fi

if [ "${DOCKER_DEPLOY_TYPE}" = "HOST" ] ; then
	echo "skip /etc/rc.local: DOCKER_DEPLOY_TYPE=${DOCKER_DEPLOY_TYPE}"
	exit
fi