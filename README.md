#### How to set up:

* sign up for ACI if you haven't already at https://accounts.aci.ics.psu.edu/acipriv/
* make sure that you are memeber of the group <code>dfc13_collab</code>
* make sure that I have added your username to the config

You don't actually need to clone this repo; for enabling the queue sharing, simply do the following:

* Run via ssh the following command on aci-b:
<code>$ ssh USERNAME@aci-b.aci.ics.psu.edu '~pde3/openQ/deploy.sh'</code>

A directory structure in your home folder is set up under <code>~/PBS</code>, containing the sub-directories:

* <code>job_pool</code>: this is where you place your *.pbs scripts for excecution. They will be automatically picked up from there.
* <code>submitted</code>: after a job was picked up by a worker, it is moved here
* <code>output</code>: an output directory where your jobs can write output files into
* <code>log</code>: an output directory where your jobs can write logging files into
* <code>qsub_info</code>: an output directory where messages or failures from qsub commands are placed
* <code>tmp</code>: Job files are temporarily moved here while the worker attempts to submit them to help ensure multiple workers don't try to submit the same jobs

#### How to create your jobs:

There is an example of how to create jobs in this repository. You can use the two files <code>job.in</code> and <code>make_pbs_scripts.sh</code>. This creates some dummy jobs for testing. Please note a few important things:
* all files created by the job must be made group-readable! This is done in the example using <code>chgrp</code> and <code>chmod</code>. (If you don't do this, you'll end up with files in your output directories, that are not accessibe to you)
* all files that your job requires (your source code for instance, or input files) must also be group readable and maybe excecutable. If this is not the case, all jobs will fail.
* don't use <code>~</code> or <code>$HOME</code> to refer to your home directory in your scripts; always use absolute paths.

The example jobs can be generated, after changing the paths in <code>make_pbs_scripts.sh</code>, simply by excecuting <code>$ ./make_pbs_scripts.sh</code>
