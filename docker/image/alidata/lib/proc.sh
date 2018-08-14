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
