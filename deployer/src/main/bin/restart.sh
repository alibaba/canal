#!/bin/bash

args=$@

case $(uname) in
Linux)
  bin_abs_path=$(readlink -f $(dirname $0))
  ;;
*)
  bin_abs_path=$(cd $(dirname $0) ||exit ; pwd)
  ;;
esac

sh "$bin_abs_path"/stop.sh $args
sh "$bin_abs_path"/startup.sh $args
