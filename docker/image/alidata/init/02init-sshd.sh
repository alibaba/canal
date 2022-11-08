#!/bin/bash

# set port
if [ -z "${SSHD_PORT}" ] ; then
	SSHD_PORT=22
	[ "${DOCKER_DEPLOY_TYPE}" = "HOST" ] && SSHD_PORT=2222
fi

sed -r -i '/^OPTIONS=/ d' /etc/sysconfig/sshd
echo 'OPTIONS="-p '"${SSHD_PORT}"'"' >> /etc/sysconfig/sshd

# set admin ssh pulic key
if [ "${USE_ADMIN_PASSAGE}" = "YES" ] ; then
    echo "set admin passage"
    mkdir -p /home/admin/.ssh
    chown admin:admin /home/admin/.ssh
    chown admin:admin /home/admin/.ssh/authorized_keys
    chmod 644 /home/admin/.ssh/authorized_keys
fi
