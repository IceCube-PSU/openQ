#!/bin/bash

ORIG_DIR=$( pwd )
SRC_DIR=$( readlink -f $( dirname $0 ) )
echo "Source directory: \"$SRC_DIR\""
DIST_DIR="$SRC_DIR/dist"
GROUP_BUILD_DIR="/gpfs/group/dfc13/default/build"
GROUP_BIN_DIR="/gpfs/group/dfc13/default/usr/bin"

for DIR in "$DIST_DIR" "$GROUP_BUILD_DIR" "$GROUP_BIN_DIR"
do
	if [ ! -d "$DIR" ]
	then
		mkdir -p "$DIR" || exit 1
	fi
	chgrp -R dfc13_collab "$DIR" || exit 1
	chmod -R 2770 "$DIR" || exit 1
done

cd "$SRC_DIR"

rm -rf \
	$SRC_DIR/daemon.spec \
	$SRC_DIR/oqstat.spec \
	$SRC_DIR/build \
	$SRC_DIR/dist/daemon \
	$SRC_DIR/dist/oqstat \
	$SRC_DIR/*.pyc \
	2>/dev/null

# NOTE: --onefile doesn't work due to daemon behavior (fork() causes helper
# binaries to be lost that are in the zip archive)
pyi-makespec daemon.py \
    --key='dkci(jjvuyu3j8*(' || exit 1

pyi-makespec qstat.py \
	--name oqstat \
	--exclude-module matplotlib \
	--exclude-module gtk \
	--exclude-module PySide \
	--exclude-module PyQt4 \
	--exclude-module PyQt5 \
	--exclude-module llvmlite \
	--exclude-module scipy \
	--exclude-module Cython \
	--exclude-module scipy.sparse \
	--exclude-module scipy.spatial.qhull \
    || exit 1

pyinstaller daemon.spec \
    --distpath "$DIST_DIR" \
    --noconfirm || exit 1

pyinstaller oqstat.spec \
    --distpath "$DIST_DIR" \
    --noconfirm || exit 1
echo "$DIST_DIR/oqstat/oqstat"

#
# "Install" tools (but _not_ daemon) in group dirs
#

# "Install" oqstat
if "$DIST_DIR/oqstat/oqstat" >/dev/null 2>&1
then
	echo "Copying built files & libraries to directory \"$GROUP_BUILD_DIR/oqstat\""
	cp -ar "$DIST_DIR/oqstat" "$GROUP_BUILD_DIR/"
	chmod -R g=u "$GROUP_BUILD_DIR/oqstat"

	echo "Placing link to executable at \"$GROUP_BIN_DIR/oqstat\""
	rm -f "$GROUP_BIN_DIR/oqstat" 2>/dev/null
	ln -s "$GROUP_BUILD_DIR/oqstat/oqstat" "$GROUP_BIN_DIR/oqstat"
else
	echo "Built oqstat didn't run successfully, not installing."
fi

# "Install" report_daemons.sh
cp "$SRC_DIR/report_daemons.sh" "$GROUP_BIN_DIR"


cd "$ORIG_DIR"
