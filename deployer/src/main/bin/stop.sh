#!/bin/bash

cygwin=false;
case "`uname`" in
    CYGWIN*)
        cygwin=true
        ;;
esac

get_pid() {	
	STR=$1
    if $cygwin; then
        JAVA_CMD="$JAVA_HOME\bin\java"
        JAVA_CMD=`cygpath --path --unix $JAVA_CMD`
        JAVA_PID=`ps |grep $JAVA_CMD |awk '{print $1}'`
    else
        STR=`ps -C java -f --width 1000 | grep "$STR"`
        if [ ! -z "$STR" ]; then
            JAVA_PID=`ps -C java -f --width 1000|grep "$STR"|grep -v grep|awk '{print $2}'`
        fi
    fi
    echo $JAVA_PID;
}

base=`dirname $0`/..
pid=`cat $base/bin/canal.pid`
if [ "$pid" == "" ] ; then
	pid=`get_pid "appName=otter-canal"`
fi

echo -e "`hostname`: stopping canal $pid ... "
kill $pid

LOOPS=0
while (true); 
do 
	pid=`get_pid "appName=otter-canal"`
    if [ "$pid" == "" ] ; then
    	echo "Oook! cost:$LOOPS"
    	`rm $base/bin/canal.pid`
    	break;
    fi
    let LOOPS=LOOPS+1
    sleep 1
done