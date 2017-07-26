#!/bin/bash

srcdir=$( dirname $0 )

cd "$srcdir"

rm -rf daemon.spec build dist 2>/dev/null

pyinstaller daemon.py --distpath ./dist --noconfirm --onefile --key='dkci(jjvuyu3j8*('

chgrp -R dfc13_collab ./dist
chmod -R g+rx ./dist

cd -
