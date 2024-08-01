#!/bin/bash
set +e

source /etc/profile
export JAVA_HOME=/usr/java/latest
export PATH=$JAVA_HOME/bin:$PATH
touch /tmp/start.log
chown admin: /tmp/start.log
chown -R admin: /home/admin/canal-admin
host=`hostname -i`

MYSQL_USER_PASSWORD=`perl -le 'print $ENV{"spring.datasource.password"}'`
MYSQL_USER=`perl -le 'print $ENV{"spring.datasource.username"}'`
MYSQL_DATABASE=`perl -le 'print $ENV{"spring.datasource.database"}'`
MYSQL_ADDRESS=`perl -le 'print $ENV{"spring.datasource.address"}'`

if [ -z "${MYSQL_USER_PASSWORD}" ]; then
    MYSQL_USER_PASSWORD="canal"
fi
if [ -z "${MYSQL_USER}" ]; then
    MYSQL_USER="canal"
fi
if [ -z "${MYSQL_DATABASE}" ]; then
    MYSQL_DATABASE="canal_manager"
fi

# waitterm
#   wait TERM/INT signal.
#   see: http://veithen.github.io/2014/11/16/sigterm-propagation.html
waitterm() {
        local PID
        # any process to block
        tail -f /dev/null &
        PID="$!"
        # setup trap, could do nothing, or just kill the blocker
        trap "kill -TERM ${PID}" TERM INT
        # wait for signal, ignore wait exit code
        wait "${PID}" || true
        # clear trap
        trap - TERM INT
        # wait blocker, ignore blocker exit code
        wait "${PID}" 2>/dev/null || true
}

# waittermpid "${PIDFILE}".
#   monitor process by pidfile && wait TERM/INT signal.
#   if the process disappeared, return 1, means exit with ERROR.
#   if TERM or INT signal received, return 0, means OK to exit.
waittermpid() {
        local PIDFILE PID do_run error
        PIDFILE="${1?}"
        do_run=true
        error=0
        trap "do_run=false" TERM INT
        while "${do_run}" ; do
                PID="$(cat "${PIDFILE}")"
                if ! ps -p "${PID}" >/dev/null 2>&1 ; then
                        do_run=false
                        error=1
                else
                        sleep 1
                fi
        done
        trap - TERM INT
        return "${error}"
}


function checkStart() {
    local name=$1
    local cmd=$2
    local timeout=$3
    cost=5
    while [ $timeout -gt 0 ]; do
        ST=`eval $cmd`
        if [ "$ST" == "0" ]; then
            sleep 1
            let timeout=timeout-1
            let cost=cost+1
        elif [ "$ST" == "" ]; then
            sleep 1
            let timeout=timeout-1
            let cost=cost+1
        else
            break
        fi
    done
    echo "start $name successful"
}

function start_mysql() {
    echo "start mysql ..."
    # start mysql
    MYSQL_ROOT_PASSWORD=Hello1234
    # connect local mysql
    if [ -z "$(ls -A /var/lib/mysql)" ]; then
        TEMP_FILE='/tmp/init.sql'
        echo "ALTER USER 'root'@'localhost' IDENTIFIED BY '${MYSQL_ROOT_PASSWORD}';" >> $TEMP_FILE
        /usr/sbin/mysqld --initialize --user=mysql --datadir=/var/lib/mysql  --init-file=/tmp/init.sql --default-authentication-plugin=mysql_native_password 1>>/tmp/start.log 2>&1
        echo "default-authentication-plugin=mysql_native_password" >> /etc/my.cnf
        # systemctl start mysqld
        sudo -u mysql /usr/sbin/mysqld &
        sleep 5
        checkStart "mysql" "echo 'show status' | mysql -b -s -h127.0.0.1 -P3306 -uroot -p${MYSQL_ROOT_PASSWORD} | grep -c Uptime" 30
        # init file
        rm -f $TEMP_FILE
        echo "create database if not exists $MYSQL_DATABASE ;" >> $TEMP_FILE
        echo "create user $MYSQL_USER identified by '$MYSQL_USER_PASSWORD' ;" >> $TEMP_FILE
        echo "grant all privileges on $MYSQL_DATABASE.* to '$MYSQL_USER'@'%' ;" >> $TEMP_FILE
        echo "flush privileges;" >> $TEMP_FILE
        # init user
        cmd="mysql -h127.0.0.1 -uroot -p${MYSQL_ROOT_PASSWORD} -e 'source $TEMP_FILE' 1>>/tmp/start.log 2>&1"
        eval $cmd
        /bin/rm -f /tmp/init.sql
        # init table
        cmd="mysql -h127.0.0.1 -u$MYSQL_USER -p$MYSQL_USER_PASSWORD $MYSQL_DATABASE -e 'source /home/admin/canal-admin/conf/canal_manager.sql' 1>>/tmp/start.log 2>&1"
        eval $cmd
        /bin/rm -f /home/admin/canal-admin/conf/canal_manager.sql
    else
        echo "recover mysql ..."
        chown -R mysql:mysql /var/lib/mysql
        # systemctl start mysqld
        rm -f /var/lib/mysql/mysql.sock.lock
        sudo -u mysql /usr/sbin/mysqld &
        sleep 5
        #check start
        checkStart "mysql" "echo 'show status' | mysql -b -s  -h127.0.0.1 -P3306 -uroot -p${MYSQL_ROOT_PASSWORD} | grep -c Uptime" 30
    fi
}

function stop_mysql() {
    echo "stop mysql ..."
    # stop mysql
    # systemctl stop mysqld
    ps auxf | grep mysqld | grep -v grep | awk '{print $2}' | xargs kill
    echo "stop mysql successful ..."
}

function start_admin() {
    echo "start admin ..."
    serverPort=`perl -le 'print $ENV{"server.port"}'`
    if [ -z "$serverPort" ] ; then
        serverPort=8089
    fi
    su admin -c 'cd /home/admin/canal-admin/bin/ && sh restart.sh 1>>/tmp/start.log 2>&1'
    sleep 5
    #check start
    checkStart "canal" "nc 127.0.0.1 $serverPort -w 1 -zv 2>/tmp/nc.out && cat /tmp/nc.out | grep -c Connected" 30
}

function stop_admin() {
    echo "stop admin"
    su admin -c 'cd /home/admin/canal-admin/bin/ && sh stop.sh 1>>/tmp/start.log 2>&1'
    echo "stop admin successful ..."
}

echo "==> START ..."

if [ -z "${MYSQL_ADDRESS}" ]; then
    start_mysql
fi
start_admin

echo "==> START SUCCESSFUL ..."

tail -f /dev/null &
# wait TERM signal
waitterm

echo "==> STOP"

if [ -z "${MYSQL_ADDRESS}" ]; then
    stop_admin
fi
stop_mysql

echo "==> STOP SUCCESSFUL ..."
