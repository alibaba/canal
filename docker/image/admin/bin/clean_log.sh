#!/bin/bash

# Global Settings
PATH="$HOME/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/usr/X11R6/bin:/root/bin"
export PATH

CUTOFF="85"
#获取磁盘使用率最高的分区
USAGE=$(df -h|awk 'NR>1 {gsub(/%$/,"",$5);print $5 }'|sort -nr|head -1)
before=$USAGE

baseClean(){
    #删除tmp目录15天前的文件。
    #更新文档时间戳
    if [ -d /tmp/hsperfdata_admin ]
    then
        touch /tmp/hsperfdata_admin
        touch /tmp/hsperfdata_admin/*
    fi

    find /tmp/ -type f -mtime +15 | xargs -t rm -rf >/dev/null 2>&1


    now=$(df -h|awk 'NR>1 {gsub(/%$/,"",$5);print $5 }'|sort -nr|head -1)
    echo "before:$before; now:$now"
}

CANAL_DIR="/home/admin/canal-server/logs"
if [ ! -d "$CANAL_DIR" ]
  CANAL_DIR="/home/admin/canal-admin/logs"
fi

if [[ -d $CANAL_DIR ]]; then
  USAGE=$(df -h|awk 'NR>1 {gsub(/%$/,"",$5);print $5 }'|sort -nr|head -1)
  if [[ $USAGE -ge 90 ]]; then
        find $CANAL_DIR -type f -mtime +7 | xargs rm -rf {}
  fi
  USAGE=$(df -h|awk 'NR>1 {gsub(/%$/,"",$5);print $5 }'|sort -nr|head -1)
  if [[ $USAGE -ge 80 ]]; then
        find $CANAL_DIR -type f -mtime +3 | xargs rm -rf {}
  fi
  USAGE=$(df -h|awk 'NR>1 {gsub(/%$/,"",$5);print $5 }'|sort -nr|head -1)
  if [[ $USAGE -ge 80 ]]; then
        find $CANAL_DIR -type d -empty -mtime +3 | grep -v canal | xargs rm -rf {}
        find $CANAL_DIR -type f -iname '*.tmp' | xargs rm -rf {}
  fi
  baseClean
  exit 0
fi