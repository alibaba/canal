#!/bin/bash

current_path=`pwd`
case "`uname`" in
    Darwin)
        bin_abs_path=`cd $(dirname $0); pwd`
        ;;
    Linux)
        bin_abs_path=$(readlink -f $(dirname $0))
        ;;
    *)
        bin_abs_path=`cd $(dirname $0); pwd`
        ;;
esac
BASE=${bin_abs_path}

if [ ! -f $BASE/jdk*.rpm ] ; then
    DOWNLOAD_LINK="http://download.oracle.com/otn-pub/java/jdk/8u181-b13/96a7b8442fe848ef90c96a2fad6ed6d1/jdk-8u181-linux-x64.tar.gz"
    wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=xxx; oraclelicense=accept-securebackup-cookie" "$DOWNLOAD_LINK" -O $BASE/jdk-8-linux-x64.rpm
fi

cd $BASE/../ && mvn clean package -Dmaven.test.skip -Denv=release && cd $current_path ;

if [ "$1" == "kafka" ] ; then
	cp $BASE/../target/canal-kafka-*.tar.gz $BASE/
	docker build --no-cache -t canal/canal-server $BASE/
else 
	cp $BASE/../target/canal.deployer-*.tar.gz $BASE/
	docker build --no-cache -t canal/canal-server $BASE/
fi
