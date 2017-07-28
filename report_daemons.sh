#!/bin/bash

printf "   %-8s  %-51s  %5s  %6s\n" User Host PID "Daemon"
printf "   %-8s  %-51s  %5s  %6s\n" "--------" "---------------------------------------------------" "-----" "------"

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
	  	suffix="No"
	else
		prefix="o"
	  	suffix="Yes"
	fi
	printf " %1s %-8s  %-51s  %5d  %6s\n" "$prefix" "$u" "$host" "$pid" "$suffix"
done
