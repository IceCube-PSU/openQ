#!/usr/bin/env bash

logdir=/storage/home/pde3/PBS/log
outdir=/storage/home/pde3/PBS/output

for run in {100..299}
do
    `sed -e "s|@@RUN@@|\${run}|g"\
     -e "s|@@LOGDIR@@|\${logdir}|g"\
     -e "s|@@OUTDIR@@|\${outdir}|g"\
     job.in > run_${run}.pbs`
     chgrp dfc13_collab run_${run}.pbs
     chmod g+r run_${run}.pbs
done
