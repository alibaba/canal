#!/bin/bash
# Additional line arg for current prometheus solution
case "`uname`" in
Linux)
    bin_abs_path=$(readlink -f $(dirname $0))
	;;
*)
	bin_abs_path=`cd $(dirname $0); pwd`
	;;
esac
base=${bin_abs_path}/..
if [ $(ls $base/lib/aspectjweaver*.jar | wc -l) -eq 1 ]; then
    WEAVER=$(ls $base/lib/aspectjweaver*.jar)
    METRICS_OPTS=" -javaagent:"${WEAVER}" "
fi
