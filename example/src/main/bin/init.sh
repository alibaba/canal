#! /bin/bash
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

# default values
PORT=3306
MYSQL_IP="0"

# parse arguments
while getopts "h:p:" opt; do
    case $opt in
        p)
            if [[ $OPTARG -gt 0 ]]; then
		PORT=$OPTARG
            else
                echo "Invalid -p option: -$OPTARG, should be positive integer >&2"
                exit 1
            fi
            ;;
       h)
            MYSQL_IP=$OPTARG
            ;;
        \?)
            printUsage
            exit 1
            ;;
    esac
done

function printUsage
{
    cat << __Usage

Initialise the instance config property file automatically. This tool will connect to Mysql master server and get the binlog file and position then update the instance config property file for you.

Usage: $0 -p [port] -h [hostname]
Options:
    -p: mysql port, default to 3306
    -h: mysql host ip.
    For example:
        $0 -p 3306 -h 172.17.0.2

Enjoy!!!

__Usage
}

if [[ $PORT -lt 0 ]]; then
  echo "invalid port argument $PORT"
  printUsage
  exit 1
fi

# check if mysql ip is specified
if [[ $MYSQL_IP = "0" ]]; then
  echo '-h mysql host ip argument is required.'
  printUsage
  exit 1
fi

MYSQL_ADDR="$MYSQL_IP:$PORT"

MASTER_STATUS=`mysql -ucanal -pcanal -h$MYSQL_IP -P$PORT -e 'show master status' 2>/dev/null | head -n 2 | tail -n 1`

LOG_FILE=`echo "$MASTER_STATUS" | cut -f 1`
LOG_POS=`echo "$MASTER_STATUS" | cut -f 2`

INSTANCE_CONF_FILE=$base/conf/example/instance.properties

if [ ! -f $INSTANCE_CONF_FILE ]; then
  echo "Can not find instance config file: $INSTANCE_CONF_FILE"
  exit 1
fi

# replace the binlog file and position
sed -i.bak -e "s/canal\.instance\.master\.address = .*/canal\.instance\.master\.address = $MYSQL_ADDR/" -e "s/canal\.instance\.master\.journal\.name = .*/canal\.instance\.master\.journal\.name = $LOG_FILE/" -e "s/canal\.instance\.master\.position = .*/canal\.instance\.master\.position = $LOG_POS/" $INSTANCE_CONF_FILE

cat $INSTANCE_CONF_FILE

echo
echo 'Above is your instance config file, check if the settings are correct'
echo
