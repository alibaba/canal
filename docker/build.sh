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

if [ "$1" == "base" ] ; then
    docker build --no-cache -t canal/osbase $BASE/base
elif [ "$1" == "base_v2" ] ; then
    docker build --no-cache -t canal/osbase $BASE/base -f $BASE/base/Dockerfile_v2
elif [ "$1" == "base_admin" ] ; then
    docker build --no-cache -t canal/osadmin $BASE/base -f $BASE/base/Dockerfile_admin
elif [ "$1" == "admin" ] ; then
    rm -rf $BASE/canal.*.tar.gz ;
    cd $BASE/../ && mvn clean package -Dmaven.test.skip -Denv=release && cd $current_path ;
    cp $BASE/../target/canal.admin-*.tar.gz $BASE/
    docker build --no-cache -t canal/canal-admin $BASE/ -f $BASE/Dockerfile_admin
else 
    rm -rf $BASE/canal.*.tar.gz ; 
    cd $BASE/../ && mvn clean package -Dmaven.test.skip -Denv=release && cd $current_path ;
    cp $BASE/../target/canal.deployer-*.tar.gz $BASE/
    docker build --no-cache -t canal/canal-server $BASE/
    sudo docker login --username=rainbow954@163.com registry.cn-hangzhou.aliyuncs.com --password dockerhub@iotbull.com
    sudo docker tag canal/canal-server:latest canal/canal-server:v1.1.3
    sudo docker push registry.cn-hangzhou.aliyuncs.com/rainbow954/canal-server:v1.1.3

fi
