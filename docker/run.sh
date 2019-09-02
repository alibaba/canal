#!/bin/bash

function usage() {
    echo "Usage:"
    echo "  run.sh [CONFIG]"
    echo "example 1 :"
    echo "  run.sh -e canal.instance.master.address=127.0.0.1:3306 \\"
    echo "         -e canal.instance.dbUsername=canal \\"
    echo "         -e canal.instance.dbPassword=canal \\"
    echo "         -e canal.instance.connectionCharset=UTF-8 \\"
    echo "         -e canal.instance.tsdb.enable=true \\"
    echo "         -e canal.instance.gtidon=false \\"
    echo "         -e canal.instance.filter.regex=.*\\\\\\..* "
    echo "example 2 :"
    echo "  run.sh -e canal.admin.manager=127.0.0.1:8089 \\"
    echo "         -e canal.admin.port=11110 \\"
    echo "         -e canal.admin.user=admin \\"
    echo "         -e canal.admin.passwd=4ACFE3202A5FF5CF467898FC58AAB1D615029441"
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
#VOLUMNS="-v $DATA:/home/admin/canal-server/logs"
PORTLIST="11110 11111 11112 9100"
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
        bin_abs_path=`cd $(dirname $0); pwd`
        NET_MODE="--net=host"
        PORTS=""
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



MEMORY="-m 4096m"
LOCALHOST=`getMyIp`
cmd="docker run -d -it -h $LOCALHOST $CONFIG --name=canal-server $VOLUMNS $NET_MODE $PORTS $MEMORY canal/canal-server"
echo $cmd
eval $cmd