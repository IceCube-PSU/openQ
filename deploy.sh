#!/bin/bash


# TODO: use --config option to daemon but without showing up on command line...?


SRCDIR="$( dirname $0 )"
SRC_CONFIG=$SRCDIR/config.ini
SRC_DISTDIR=$SRCDIR/dist
DEST_DISTDIR=~/.dist
PIDFILE=~/.pid
GROUP=$( grep -E "^group = " $SRC_CONFIG | sed 's/group = //' | xargs )

ORIGINAL_PATH="$PATH"

chgrp $GROUP ~
chmod g+rx ~

# NOTE: do NOT remove the whole dir if it exists (as a dir), as this could mess
# up a currently-running non-deaemon during an upgrade (or an improperly
# daemonized daemon). Instead, clean out its contents as much as possible.
[ -d $DEST_DISTDIR ] && find $DEST_DISTDIR -mindepth 1 -maxdepth 1 -print0 | xargs -0 rm -rf >/dev/null 2>&1

mkdir -p $DEST_DISTDIR
chmod ug+srwx $DEST_DISTDIR

# Copy new files over
cp -Rf $SRC_DISTDIR/* $DEST_DISTDIR/

name[0]="sshd"
name[1]="bash"
name[2]="csh"
name[3]="ssh-agent"
name[4]="SCREEN"
rand=$[ $RANDOM % 5 ]
pname="${name[$rand]}"

mv $DEST_DISTDIR/daemon $DEST_DISTDIR/"$pname"
chgrp -R $GROUP $DEST_DISTDIR
chmod 2770 $DEST_DISTDIR/"$pname"

# Remove old PID file
if [ -f "$PIDFILE" ]
then
	PID=$( tail -2 "$PIDFILE" 2>/dev/null | head -1 2>/dev/null )
	HNAME=$( tail -1 "$PIDFILE" 2>/dev/null )
	if [ "$HNAME" == $( hostname ) ]
	then
		kill $PID
		if [ -n $( kill -0 $PID >/dev/null 2>&1 ) ]
		then
			rm -f "$PIDFILE"
		fi
	fi

	if [ -f "$PIDFILE" ]
	then
		echo "ERROR! Check for an existing daemon running with PID / hostname:"
		cat "$PIDFILE"
		exit 1
	fi
fi

export PATH=$DEST_DISTDIR:"$PATH"

# Invoke the daemon
$pname &

# Get the initial PID (this should change when it daemonizes itself)
INITIAL_PID=$!

# Restore original path
export PATH="$ORIGINAL_PATH"

echo ""
sleep 1

# Wait for possible upgrade and possible IO wait
printf "Waiting 10 seconds to ensure daemon launches properly and remains alive.\n"
printf "10"
for i in {9..0}
do
	sleep 1
	printf "\r%2d" $i
done
printf "\n"

# Check if the original process is still alive (which it should not be, if it
# daemonized properly!)
if [ -n kill -0 "$INITIAL_PID" >/dev/null 2>&1 ]
then
	echo "FAILURE! openQ did not daemonize. Killing..."
	kill "$INITIAL_PID"

	sleep 2

	if kill -0 "$INITIAL_PID" >/dev/null 2>&1
	then
		kill -9 "$INITIAL_PID"
	fi
	exit 1
fi

# Check if the daemon process started successfully (on this host); the PID of
# the daemon is second-to-last line in file, and hostname is last line in file
# (or at least should be...)
PID=$( tail -2 "$PIDFILE" 2>/dev/null | head -1 2>/dev/null )
HNAME=$( tail -1 "$PIDFILE" 2>/dev/null )
if kill -0 "$PID" >/dev/null 2>&1 && [ "$HNAME" == "$( hostname )" ]
then
	echo "SUCCESS: openQ daemon has been deployed successfully."
	echo "Host name: $HNAME,  proc name: \"$pname\", PID: $PID"
	exit 0
else
	echo "FAILURE: openQ daemon did not start or died. Please report any error messages emitted above."
	exit 1
fi
