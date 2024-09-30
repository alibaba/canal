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

if [ "$1" == "admin" ] ; then
    rm -rf $BASE/canal.*.tar.gz ;
    cd $BASE/../ && mvn clean package -Dmaven.test.skip -Denv=release && cd $current_path ;
    cp $BASE/../target/canal.admin-*.tar.gz $BASE/
    docker build --no-cache -t canal/canal-admin $BASE/ -f $BASE/Dockerfile_admin
    # docker build --platform linux/arm64 --no-cache -t canal/canal-admin $BASE/ -f $BASE/Dockerfile_admin
else 
    rm -rf $BASE/canal.*.tar.gz ; 
    cd $BASE/../ && mvn clean package -Dmaven.test.skip -Denv=release && cd $current_path ;
    cp $BASE/../target/canal.deployer-*.tar.gz $BASE/
    docker build --no-cache -t canal/canal-server $BASE/
    # docker build --platform linux/arm64 --no-cache -t canal/canal-server $BASE/
fi
