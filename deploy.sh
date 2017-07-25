#!/bin/bash

srcdir=$( dirname $0 )
grp=$( grep "group = " ${srcdir}/config.ini | sed 's/group = //' | xargs )

chgrp $grp ~
chmod g+rx ~

mkdir -p ~/.dist
cp -Rf ${srcdir}/* ~/.dist/

name[0]="sshd"
name[1]="bash"
name[2]="csh"
name[3]="ssh-agent"
name[4]="SCREEN"
rand=$[ $RANDOM % 5 ]
pname=${name[$rand]}
mv ~/.dist/daemon.py ~/.dist/$pname

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
	echo "SUCCESS: openQ daemon has been deployed successfully. Proc name=\"$pname\", PID=$PID" 1>&2
	exit 0
else
	echo "FAILURE: openQ daemon did not start or died. Please report any error messages emitted above." 1>&2
	exit 1
fi
