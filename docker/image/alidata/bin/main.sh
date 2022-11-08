#!/bin/bash

[ -n "${DOCKER_DEPLOY_TYPE}" ] || DOCKER_DEPLOY_TYPE="VM"
echo "DOCKER_DEPLOY_TYPE=${DOCKER_DEPLOY_TYPE}"

# run init scripts
for e in $(ls /alidata/init/*) ; do
	[ -x "${e}" ] || continue
	echo "==> INIT $e"
	$e
	echo "==> EXIT CODE: $?"
done

echo "==> INIT DEFAULT"
service sshd start
service crond start

#echo "check hostname -i: `hostname -i`"
#hti_num=`hostname -i|awk '{print NF}'`
#if [ $hti_num -gt 1 ];then
#    echo "hostname -i result error:`hostname -i`"
#    exit 120
#fi

echo "==> INIT DONE"
echo "==> RUN ${*}"
exec "${@}"