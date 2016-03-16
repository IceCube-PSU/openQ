#!/bin/bash
chgrp dfc13_collab ~
chmod g+rx ~
cp /storage/home/pde3/openQ/daemon .
./daemon > /dev/null 2>&1 &
PID=$!
disown -h %-
echo "kill -9 ${PID}" > kill_daemon.sh
chmod +x kill_daemon.sh
