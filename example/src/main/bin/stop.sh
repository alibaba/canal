#!/bin/bash

cygwin=false;
case "`uname`" in
    CYGWIN*)
        cygwin=true
        ;;
esac

get_pid() {	
	STR=$1
	PID=$2
    if $cygwin; then
        JAVA_CMD="$JAVA_HOME\bin\java"
        JAVA_CMD=`cygpath --path --unix $JAVA_CMD`
        JAVA_PID=`ps |grep $JAVA_CMD |awk '{print $1}'`
    else
        if [ ! -z "$PID" ]; then
        	JAVA_PID=`ps -C java -f --width 1000|grep "$STR"|grep "$PID"|grep -v grep|awk '{print $2}'`
	    else 
	        JAVA_PID=`ps -C java -f --width 1000|grep "$STR"|grep -v grep|awk '{print $2}'`
        fi
    fi
    echo $JAVA_PID;
}

base=`dirname $0`/..
pidfile=$base/bin/canal.pid
if [ ! -f "$pidfile" ];then
	echo "canal is not running. exists"
	exit
fi

pid=`cat $pidfile`
if [ "$pid" == "" ] ; then
	pid=`get_pid "appName=otter-canal-example"`
fi

echo -e "`hostname`: stopping canal $pid ... "
kill $pid

LOOPS=0
while (true); 
do 
	gpid=`get_pid "appName=otter-canal-example" "$pid"`
    if [ "$gpid" == "" ] ; then
    	echo "Oook! cost:$LOOPS"
    	`rm $pidfile`
    	break;
    fi
    let LOOPS=LOOPS+1
    sleep 1
done