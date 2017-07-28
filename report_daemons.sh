#!/bin/bash

printf "%2s %-8s  %-51s  %5s\n" "D?" "User" "Host" "PID"
printf "%2s %-8s  %-51s  %5s\n" "--" "--------" "---------------------------------------------------" "-----"

up_count=0
down_count=0
for u in $( grep "list = " ~jll1062/openQ/config.ini | sed -e 's/list = //' -e 's/,/\n/g' | sort | xargs )
do
	num_lines=$( cat ~/../${u}/.pid 2>/dev/null | wc -l 2>/dev/null )
	if [ -z "$num_lines" -o "$num_lines" != "2" ]
	then
		output=""
	else
		pid=$( head -1 ~/../${u}/.pid 2>/dev/null )
		host=$( tail -1 ~/../${u}/.pid 2>/dev/null )

		if [ -z "$host" -o -z "$pid" ]
		then
			output=""
		elif [ "$host" != "$HOSTNAME" ]
		then
			output=$( ssh $host "ps h $pid" )
		else
			output=$( ps h $pid )
		fi
	fi

	if [ -z "$output" ]
	then
		prefix=" "
		down_count=$(( down_count + 1))
	else
		prefix="o"
		up_count=$(( up_count + 1))
	fi
	printf "%2s %-8s  %-51s  %5d\n" "$prefix" "$u" "$host" "$pid"
done
printf '\nTotal: %d openQ users; %d running and %d stopped.\n\n' "$(( up_count + down_count ))" "$up_count" "$down_count"
