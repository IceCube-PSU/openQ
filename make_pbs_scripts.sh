#!/usr/bin/env bash


srcdir="$( dirname $0 )"
grp=$( grep -E "^group = " "$srcdir"/config.ini | sed 's/group = //' | xargs )
basedir=$( grep -E "^basedir = " "$srcdir"/config.ini | sed -e 's/basedir = //' -e "s/<!User!>/${USER}/g" | xargs )
log=$( grep -E "^log = " "$srcdir"/config.ini | sed 's/log = //' | xargs )
out=$( grep -E "^out = " "$srcdir"/config.ini | sed 's/out = //' | xargs )

logdir=$basedir/$log
outdir=$basedir/$out

for run in {0..99}
do
    `sed -e "s|@@RUN@@|\${run}|g" \
		-e "s|@@LOGDIR@@|\${logdir}|g" \
		-e "s|@@OUTDIR@@|\${outdir}|g" \
		job.in > run_${run}.pbs`
     chgrp $grp run_${run}.pbs
     chmod g+r run_${run}.pbs
done
