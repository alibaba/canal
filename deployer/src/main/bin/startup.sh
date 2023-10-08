#!/bin/bash 

current_path=`pwd`
case "`uname`" in
    Linux)
		bin_abs_path=$(readlink -f $(dirname $0))
		;;
	*)
		bin_abs_path=`cd $(dirname $0); pwd`
		;;
esac
base=${bin_abs_path}/..
canal_conf=$base/conf/canal.properties
canal_local_conf=$base/conf/canal_local.properties
logback_configurationFile=$base/conf/logback.xml
export LANG=en_US.UTF-8
export BASE=$base

if [ -f $base/bin/canal.pid ] ; then
	echo "found canal.pid , Please run stop.sh first ,then startup.sh" 2>&2
    exit 1
fi

if [ ! -d $base/logs/canal ] ; then 
	mkdir -p $base/logs/canal
fi

## set java path
if [ -z "$JAVA" ] ; then
  JAVA=$(which java)
fi

ALIBABA_JAVA="/usr/alibaba/java/bin/java"
TAOBAO_JAVA="/opt/taobao/java/bin/java"
if [ -z "$JAVA" ]; then
  if [ -f $ALIBABA_JAVA ] ; then
  	JAVA=$ALIBABA_JAVA
  elif [ -f $TAOBAO_JAVA ] ; then
  	JAVA=$TAOBAO_JAVA
  else
  	echo "Cannot find a Java JDK. Please set either set JAVA or put java (>=1.5) in your PATH." 2>&2
    exit 1
  fi
fi

case "$#" 
in
0 ) 
	;;
1 )	
	var=$*
	if [ "$var" = "local" ]; then
		canal_conf=$canal_local_conf
	else
		if [ -f $var ] ; then 
			canal_conf=$var
		else
			echo "THE PARAMETER IS NOT CORRECT.PLEASE CHECK AGAIN."
			exit
		fi
	fi;;
2 )	
	var=$1
	if [ "$var" = "local" ]; then
		canal_conf=$canal_local_conf
	else
		if [ -f $var ] ; then
			canal_conf=$var
		else 
			if [ "$1" = "debug" ]; then
				DEBUG_PORT=$2
				DEBUG_SUSPEND="n"
				JAVA_DEBUG_OPT="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=$DEBUG_PORT,server=y,suspend=$DEBUG_SUSPEND"
			fi
		fi
     fi;;
* )
	echo "THE PARAMETERS MUST BE TWO OR LESS.PLEASE CHECK AGAIN."
	exit;;
esac

JavaVersion=`$JAVA -version 2>&1 |awk 'NR==1{ gsub(/"/,""); print $3 }' | awk  -F '.' '{print $1}'`
str=`file -L $JAVA | grep 64-bit`
JAVA_OPTS="$JAVA_OPTS -Xss1m -XX:+AggressiveOpts -XX:-UseBiasedLocking -XX:-OmitStackTraceInFastThrow -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$base/logs"

if [ $JavaVersion -ge 11 ] ; then
  #JAVA_OPTS="$JAVA_OPTS -Xlog:gc*:$base_log/gc.log:time "
  JAVA_OPTS="$JAVA_OPTS"
else
  #JAVA_OPTS="$JAVA_OPTS -Xloggc:$base/logs/canal/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime"
  JAVA_OPTS="$JAVA_OPTS -XX:+UseFastAccessorMethods -XX:+PrintAdaptiveSizePolicy -XX:+PrintTenuringDistribution"
fi

if [ -n "$str" ]; then
  if [ $JavaVersion -ge 11 ] ; then
    # For G1
    JAVA_OPTS="-server -Xms2g -Xmx3g -XX:+UseG1GC -XX:MaxGCPauseMillis=250 -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent $JAVA_OPTS"
  else
	  JAVA_OPTS="-server -Xms2g -Xmx3g -Xmn1g -XX:SurvivorRatio=2 -XX:PermSize=96m -XX:MaxPermSize=256m -XX:MaxTenuringThreshold=15 -XX:+DisableExplicitGC $JAVA_OPTS"
	fi
else
	JAVA_OPTS="-server -Xms1024m -Xmx1024m -XX:NewSize=256m -XX:MaxNewSize=256m -XX:MaxPermSize=128m $JAVA_OPTS"
fi

JAVA_OPTS=" $JAVA_OPTS -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8"
CANAL_OPTS="-DappName=otter-canal -Dlogback.configurationFile=$logback_configurationFile -Dcanal.conf=$canal_conf"

if [ -e $canal_conf -a -e $logback_configurationFile ]
then 
	
	for i in $base/lib/*;
		do CLASSPATH=$i:"$CLASSPATH";
	done
 	CLASSPATH="$base/conf:$CLASSPATH";
 	
 	echo "cd to $bin_abs_path for workaround relative path"
  	cd $bin_abs_path
 	
	echo LOG CONFIGURATION : $logback_configurationFile
	echo canal conf : $canal_conf 
	echo CLASSPATH :$CLASSPATH
	$JAVA $JAVA_OPTS $JAVA_DEBUG_OPT $CANAL_OPTS -classpath .:$CLASSPATH com.alibaba.otter.canal.deployer.CanalLauncher 1>>$base/logs/canal/canal_stdout.log 2>&1 &
	echo $! > $base/bin/canal.pid 
	
	echo "cd to $current_path for continue"
  	cd $current_path
else 
	echo "canal conf("$canal_conf") OR log configration file($logback_configurationFile) is not exist,please create then first!"
fi
