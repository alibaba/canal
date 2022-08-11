#!/bin/bash

function usage() {
    echo "Usage:"
    echo "  run_admin.sh [CONFIG]"
    echo "example :"
    echo "  run_admin.sh -e server.port=8089 \\"
    echo "         -e canal.adminUser=admin \\"
    echo "         -e canal.adminPasswd=admin"
    exit
}

function check_port() {
    local port=$1
    local TL=$(which telnet)
    if [ -f $TL ]; then
        data=`echo quit | telnet 127.0.0.1 $port| grep -ic connected`
        echo $data
        return
    fi

    local NC=$(which nc)
    if [ -f $NC ]; then
        data=`nc -z -w 1 127.0.0.1 $port | grep -ic succeeded`
        echo $data
        return
    fi
    echo "0"
    return
}

function getMyIp() {
    case "`uname`" in
        Darwin)
         myip=`echo "show State:/Network/Global/IPv4" | scutil | grep PrimaryInterface | awk '{print $3}' | xargs ifconfig | grep inet | grep -v inet6 | awk '{print $2}'`
         ;;
        *)
         myip=`ip route get 1 | awk '{print $NF;exit}'`
         ;;
  esac
  echo $myip
}

CONFIG=${@:1}
#VOLUMNS="-v $DATA:/home/admin/canal-admin/logs"
PORTLIST="8089"
PORTS=""
for PORT in $PORTLIST ; do
    #exist=`check_port $PORT`
    exist="0"
    if [ "$exist" == "0" ]; then
        PORTS="$PORTS -p $PORT:$PORT"
    else
        echo "port $PORT is used , pls check"
        exit 1
    fi
done

NET_MODE=""
case "`uname`" in
    Darwin)
        bin_abs_path=`cd $(dirname $0); pwd`
        ;;
    Linux)
        bin_abs_path=$(readlink -f $(dirname $0))
        NET_MODE="--net=host"
        PORTS=""
        ;;
    *)
        NET_MODE="--net=host"
        PORTS=""
        bin_abs_path=`cd $(dirname $0); pwd`
        ;;
esac
BASE=${bin_abs_path}
DATA="$BASE/data"
mkdir -p $DATA

if [ $# -eq 0 ]; then
    usage
elif [ "$1" == "-h" ] ; then
    usage
elif [ "$1" == "help" ] ; then
    usage
fi

MEMORY="-m 1024m"
LOCALHOST=`getMyIp`
cmd="docker run -d -it -h $LOCALHOST $CONFIG --name=canal-admin $VOLUMNS $NET_MODE $PORTS $MEMORY canal/canal-admin"
echo $cmd
eval $cmd
