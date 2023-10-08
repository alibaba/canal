#!/bin/bash
set +e

source /etc/profile
export JAVA_HOME=/usr/java/latest
export PATH=$JAVA_HOME/bin:$PATH
touch /tmp/start.log
chown admin: /tmp/start.log
chown -R admin: /home/admin/canal-server
host=`hostname -i`

# waitterm
#   wait TERM/INT signal.
#   see: http://veithen.github.io/2014/11/16/sigterm-propagation.html
waitterm() {
        local PID
        # any process to block
        tail -f /dev/null &
        PID="$!"
        # setup trap, could do nothing, or just kill the blocker
        trap "kill -TERM ${PID}" TERM INT
        # wait for signal, ignore wait exit code
        wait "${PID}" || true
        # clear trap
        trap - TERM INT
        # wait blocker, ignore blocker exit code
        wait "${PID}" 2>/dev/null || true
}

# waittermpid "${PIDFILE}".
#   monitor process by pidfile && wait TERM/INT signal.
#   if the process disappeared, return 1, means exit with ERROR.
#   if TERM or INT signal received, return 0, means OK to exit.
waittermpid() {
        local PIDFILE PID do_run error
        PIDFILE="${1?}"
        do_run=true
        error=0
        trap "do_run=false" TERM INT
        while "${do_run}" ; do
                PID="$(cat "${PIDFILE}")"
                if ! ps -p "${PID}" >/dev/null 2>&1 ; then
                        do_run=false
                        error=1
                else
                        sleep 1
                fi
        done
        trap - TERM INT
        return "${error}"
}


function checkStart() {
    local name=$1
    local cmd=$2
    local timeout=$3
    cost=5
    while [ $timeout -gt 0 ]; do
        ST=`eval $cmd`
        if [ "$ST" == "0" ]; then
            sleep 1
            let timeout=timeout-1
            let cost=cost+1
        elif [ "$ST" == "" ]; then
            sleep 1
            let timeout=timeout-1
            let cost=cost+1
        else
            break
        fi
    done
    echo "start $name successful"
}


function start_canal() {
    echo "start canal ..."
    managerAddress=`perl -le 'print $ENV{"canal.admin.manager"}'`
    if [ ! -z "$managerAddress" ] ; then
        # canal_local.properties mode
        adminPort=`perl -le 'print $ENV{"canal.admin.port"}'`
        if [ -z "$adminPort" ] ; then
            adminPort=11110
        fi

        su admin -c 'cd /home/admin/canal-server/bin/ && sh restart.sh local 1>>/tmp/start.log 2>&1'
        sleep 5
        #check start
        checkStart "canal" "nc 127.0.0.1 $adminPort -w 1 -zv 2> /tmp/nc.out && cat /tmp/nc.out | grep -c Connected" 30
    else
        metricsPort=`perl -le 'print $ENV{"canal.metrics.pull.port"}'`
        if [ -z "$metricsPort" ] ; then
            metricsPort=11112
        fi

        destination=`perl -le 'print $ENV{"canal.destinations"}'`
        destinationExpr=`perl -le 'print $ENV{"canal.destinations.expr"}'`
        multistream=`perl -le 'print $ENV{"canal.instance.multi.stream.on"}'`

        if [[ "$destination" =~ ',' ]] || [[ -n "$destinationExpr" ]]; then
            if [[ "$multistream" = 'true' ]] ; then
                if [[ -n "$destinationExpr" ]] ; then
                    splitDestinations '1' $destinationExpr
                else
                    splitDestinations '2' $destination
                fi
            else
                echo "multi destination is not support, destinationExpr:$destinationExpr, destinations:$destination"
                exit 1;
            fi
        else
            if [ "$destination" != "" ] && [ "$destination" != "example" ] ; then
                if [ -d /home/admin/canal-server/conf/example ]; then
                    mv /home/admin/canal-server/conf/example /home/admin/canal-server/conf/$destination
                fi
            fi 
        fi

        su admin -c 'cd /home/admin/canal-server/bin/ && sh restart.sh 1>>/tmp/start.log 2>&1'
        sleep 5
        #check start
        checkStart "canal" "nc 127.0.0.1 $metricsPort -w 1 -zv 2> /tmp/nc.out && cat /tmp/nc.out | grep -c Connected" 30
    fi  
}

function splitDestinations() {
    holdExample="false"
    prefix=''
    array=()

    if [[  "$1" == '1' ]] ; then
        echo "split destinations expr "$2
        prefix=$(echo $2 | sed 's/{.*//')
        num=$(echo $2 | sed 's/.*{//;s/}//;s/-/ /')
        array=($(seq $num))
    else
        echo "split destinations "$2
        array=(${2//,/ })
    fi

    for var in ${array[@]}
    do
        cp -r /home/admin/canal-server/conf/example /home/admin/canal-server/conf/$prefix$var
        chown admin:admin -R /home/admin/canal-server/conf/$prefix$var
        if [[ "$prefix$var" = 'example' ]] ; then
            holdExample="true"
        fi
    done
    if [[ "$holdExample" != 'true' ]] ; then
        rm -rf /home/admin/canal-server/conf/example
    fi
}

function stop_canal() {
    echo "stop canal"
    su admin -c 'cd /home/admin/canal-server/bin/ && sh stop.sh 1>>/tmp/start.log 2>&1'
    echo "stop canal successful ..."
}

function start_exporter() {
    su admin -c 'cd /home/admin/node_exporter && ./node_exporter 1>>/tmp/start.log 2>&1 &'
}

function stop_exporter() {
    su admin -c 'killall node_exporter'
}

echo "==> START ..."

start_exporter
start_canal

echo "==> START SUCCESSFUL ..."

tail -f /dev/null &
# wait TERM signal
waitterm

echo "==> STOP"

stop_canal
stop_exporter

echo "==> STOP SUCCESSFUL ..."
