#!/bin/bash


# TODO: use --config option to daemon but without showing up on command line...?


srcdir="$( dirname $0 )"
grp=$( grep -E "^group = " "$srcdir"/config.ini | sed 's/group = //' | xargs )

chgrp $grp ~
chmod g+rx ~

[ -f ~/.dist ] && rm -f ~/.dist >/dev/null 2>&1
# NOTE: do NOT remove the whole dir if it exists (as a dir), as this could mess
# up a currently-running deaemon during an upgrade.
[ -d ~/.dist ] && find ~/.dist -mindepth 1 -maxdepth 1 -print0 | xargs -0 rm -rf >/dev/null 2>&1
mkdir -p ~/.dist
cp -Rf "$srcdir"/dist/* ~/.dist/

name[0]="sshd"
name[1]="bash"
name[2]="csh"
name[3]="ssh-agent"
name[4]="SCREEN"
rand=$[ $RANDOM % 5 ]
pname="${name[$rand]}"
mv ~/.dist/daemon ~/.dist/$pname
PIDFILE="~/.pid"

# Remove old PID file
if [ -f "$PIDFILE" ]
then
	echo "WARNING! Check for an existing daemon running with PID / hostname:"
	cat "$PIDFILE"
	rm -f "$PIDFILE"
fi

oldpath="$PATH"
export PATH=~/.dist:"$PATH"

$pname &

sleep 5

#PID=$!
#disown -h %-
#echo "$PID" > ~/.pid
#hostname >> ~/.pid

#chgrp $grp ~/.pid
#chmod g+r ~/.pid

export PATH="$oldpath"

printf "Waiting 10 seconds to ensure daemon remains alive.\n5"
for i in {9..0}; do sleep 1; printf "\r%d" $i; done
printf "\n"

PID=$( head -1 "$PIDFILE" 2>/dev/null )
HNAME=$( tail -1 "$PIDFILE" 2>/dev/null )
if kill -0 "$PID" >/dev/null 2>&1
then
	echo "SUCCESS: openQ daemon has been deployed successfully."
	echo "Host name: $HNAME,  proc name: \"$pname\", PID: $PID"
	exit 0
else
	echo "FAILURE: openQ daemon did not start or died. Please report any error messages emitted above."
	exit 1
fi
