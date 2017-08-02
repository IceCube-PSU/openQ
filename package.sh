#!/bin/bash

srcdir=$( dirname $0 )

cd "$srcdir"

rm -rf build dist/daemon *.pyc 2>/dev/null

# NOTE: --onefile doesn't work due to daemon behavior (fork() causes helper
# binaries to be lost that are in the zip archive)
pyi-makespec daemon.py \
    --key='dkci(jjvuyu3j8*(' || exit 1

pyinstaller daemon.spec \
    --distpath ./dist \
    --noconfirm || exit 1

chgrp -R dfc13_collab ./dist
chmod -R g+rx ./dist

cd -
