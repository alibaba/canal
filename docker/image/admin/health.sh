#!/bin/sh
CHECK_URL="http://127.0.0.1:8080/metrics"
CHECK_POINT="success"
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