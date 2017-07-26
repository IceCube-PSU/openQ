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

oldpath="$PATH"
export PATH=~/.dist:"$PATH"
$pname > /dev/null 2>&1 &

PID=$!
disown -h %-
echo "$PID" > ~/.pid
hostname >> ~/.pid

chgrp $grp ~/.pid
chmod g+r ~/.pid

export PATH="$oldpath"

printf "Waiting 5 seconds to ensure daemon remains alive.\n5"
for i in {4..0}; do sleep 1; printf "\r$(tput el)%d" $i; done
printf "\n"

if kill -0 $PID >/dev/null 2>&1
then
	echo "SUCCESS: openQ daemon has been deployed successfully. Proc name: \"$pname\", PID: $PID"
	exit 0
else
	echo "FAILURE: openQ daemon did not start or died. Please report any error messages emitted above."
	exit 1
fi
