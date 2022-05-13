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

rm -rf $BASE/canal.*.tar.gz ;
cd $BASE/../../ && mvn clean package -DskipTests -Denv=release && cd $current_path ;
cp $BASE/../../target/canal.adapter-*.tar.gz $BASE/
docker build --no-cache -t canal/canal-adapter:v1.1.6 $BASE/
