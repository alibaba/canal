#!/bin/sh

if [ -d "/home/admin/canal-server" ]; then
	serverPort=`perl -le 'print $ENV{"server.port"}'`
    if [ -z "$serverPort" ] ; then
        serverPort=8089
    fi

	CHECK_URL="http://127.0.0.1:$serverPort/index.html"
	CHECK_POINT="Canal"
else
	metrics_port=`perl -le 'print $ENV{"canal.metrics.pull.port"}'`
	if [ "$metrics_port" == "" ]; then
		metrics_port="11112"
	fi

	CHECK_URL="http://127.0.0.1:$metrics_port/metrics"
	CHECK_POINT="canal"	
fi

CHECK_COUNT=`curl -s --connect-timeout 7 --max-time 7 $CHECK_URL | grep -c $CHECK_POINT`
if [ $CHECK_COUNT -eq 0 ]; then
    echo "[FAILED]"
    status=0
	error=1
else
    echo "[  OK  ]"
    status=1
	error=0
fi