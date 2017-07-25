#!/usr/bin/env python


"""
Submit job files located in a directory to ACI open queue.
"""


from __future__ import absolute_import

from argparse import ArgumentParser
import ConfigParser
from datetime import datetime
from getpass import getuser
from os import listdir, rename
from os.path import getmtime, join, isdir, isfile
from random import randint
import stat
from subprocess import CalledProcessError, check_output, STDOUT
from time import sleep

from qstat import Qstat
from utils import expand, mkdir, TZ_LOCAL, wstderr, wstdout


__all__ = ['Daemon', 'parse_args', 'main']


class Daemon(object):
    """
    Queue daemon

    Parameters
    ----------
    configfile : string

    """
    def __init__(self, configfile):
        self.myusername = getuser()
        self.config = ConfigParser.ConfigParser()
        self.configfile = expand(configfile)
        self.config_time = 0
        self.qstat = None
        self.reconf()
        self.queue_stat = {'q': 0, 'r': 0, 'other': 0}

    def reconf(self):
        """If configfile has been updated, reconfigure accordingly"""
        # first check if that file was touched
        config_time = getmtime(self.configfile)
        if config_time > self.config_time:
            wstdout('reconf\n')
            self.config.read(self.configfile)
            self.users = self.config.get('Users', 'list').split(',')
            if self.myusername not in self.users:
                raise ValueError(
                    'User running script "%s" is not in users list %s'
                    % self.myusername, self.users
                )
            self.group = self.config.get('Users', 'group')
            for key, _ in self.config.items('Directories'):
                self.setup_dir(key)
            self.n_run = int(self.config.get('Queue', 'n_run'))
            self.n_queue = int(self.config.get('Queue', 'n_queue'))
            self.sleep = int(self.config.get('Queue', 'sleep'))
            self.qstat_cache_dir = expand(
                self.config.get('Logging', 'qstat_cache_dir')
            )
            self.mkdir(self.qstat_cache_dir)
            self.qstat = Qstat(stale_sec=self.sleep - 1,
                               cache_dir=self.qstat_cache_dir)
            self.config_time = config_time

    def getpath(self, dir_kind, usr):
        """Get the path to a particular kind of directory specified in the
        config file.

        Parameters
        ----------
        dir_kind : string
            Kind of dir to retrieve. E.g. 'basedir', 'job', 'sub', 'log'...

        Returns
        -------
        path : string
            Full path to the directory

        """
        basedir = expand(
            self.config.get('Directories', 'basedir').replace('<!User!>', usr)
        )
        if dir_kind == 'basedir':
            return basedir
        return join(basedir, self.config.get('Directories', dir_kind))

    def mkdir(self, path):
        """Create a directory and set ownership and permissions appropriately.

        Parameters
        ----------
        path : string
            Directory path to be created

        """
        mkdir(path, group=self.group, perms=stat.S_IRWXG | stat.S_IRWXU)

    def setup_dir(self, dir_kind):
        """Create dir of a particular kind"""
        path = self.getpath(dir_kind=dir_kind, usr=self.myusername)
        self.mkdir(path)

    @property
    def full(self):
        """bool : Is queue full?"""
        queued = 0
        running = 0
        other = 0
        for job in self.qstat.jobs:
            if (job['cluster'] != 'aci'
                    or job['queue'] != 'open'
                    or job['job_owner'] != self.myusername):
                continue
            if job['job_state'] == 'R':
                running += 1
            elif job['job_state'] == 'Q':
                queued += 1
            else:
                other += 1

        self.queue_stat['q'] = queued
        self.queue_stat['r'] = running
        self.queue_stat['o'] = other

        # decide if queue is full
        if running < self.n_run and queued < self.n_queue:
            return False

        return True

    def do_some_work(self):
        """High-level method to push jobs out"""
        # First find jobs from all users
        jobs = []
        for usr in self.users:
            dirpath = self.getpath(dir_kind='job', usr=usr)
            if not isdir(dirpath):
                continue
            jobs.extend(
                [(usr, f) for f in listdir(dirpath) if isfile(join(dirpath, f))]
            )

        # Estimate how much work is needed
        if not jobs:
            return
        free = self.n_run - self.queue_stat['r']

        # Try couple of times to find some work, then go back to sleep (outer
        # loop)
        submitted = 0
        while submitted < free:
            n_jobs = len(jobs)
            if n_jobs == 0:
                break
            # choose a job from the list
            idx = randint(0, n_jobs - 1)
            usr, job = jobs.pop(idx)
            if self.qsub(usr, job):
                submitted += 1

    def qsub(self, usr, job):
        """Submit a jobfile `job` from user `usr` to the open queue.

        Parameters
        ----------
        usr : string
            User who originally submitted the job to be run

        job : string
            Job filename

        Returns
        -------
        success : bool

        """
        job_dir = self.getpath(dir_kind='job', usr=usr)
        orig_job_filepath = join(job_dir, job)
        tmp_job_filepath = join('/tmp', job)
        try:
            rename(orig_job_filepath, tmp_job_filepath)
        except OSError:
            return False

        submitted_dir = self.getpath(dir_kind='sub', usr=usr)
        submitted_filepath = join(submitted_dir, job)

        qsub_info_dir = self.getpath(dir_kind='qsub_info', usr=usr)
        suffix = '.qsub.%s.%s' % (self.myusername,
                                  datetime.now(tz=TZ_LOCAL).isoformat())
        qsub_err_filepath = join(qsub_info_dir, job + suffix + '.err')
        qsub_out_filepath = join(qsub_info_dir, job + suffix + '.out')

        dest_filepath = submitted_filepath
        try:
            wstdout('submitting job %s from %s by %s\n'
                    % (job, usr, self.myusername))

            # TODO: figure out "-M <email>" option and place here... but might
            # want to check that this option isn't already specified in the
            # file before overrriding...

            # Note that "-m p" disables all email, and "-A open" enqueues the
            # job on the ACI open queue
            mail_options = '-m p'
            qsub_command = 'qsub %s -A open' % mail_options
            try:
                out = check_output(qsub_command.split() + [tmp_job_filepath],
                                   stderr=STDOUT)
            except CalledProcessError:
                # Want to move job file back to original location, to be
                # processed by another worker
                dest_filepath = orig_job_filepath

                # Report what went wrong with qsub command to stderr and write
                # info to a file
                err_msg = 'Failed to run command "%s"\n' % qsub_command
                if out is not None:
                    err_msg += (
                        'Output from command:\n'
                        + '\n'.join('> %s\n' % l for l in out.split('\n'))
                    )
                wstderr(err_msg + '\n')
                with open(qsub_err_filepath, 'w') as f:
                    f.write(err_msg)
                return False

            # Write qsub message(s) to file (esp. what job_id got # assigned)
            with open(qsub_out_filepath, 'w') as f:
                f.write(out)
            return True

        finally:
            try:
                rename(tmp_job_filepath, dest_filepath)
            except OSError:
                wstderr('WARNING: Could not move "%s" to "%s"' %
                        (tmp_job_filepath, dest_filepath))

    def serve_forever(self):
        """Main loop"""
        while True:
            if not self.full:
                self.do_some_work()
            wstdout('going to sleep for %s seconds...\n' % self.sleep)
            sleep(self.sleep)
            self.reconf()


def parse_args(description=__doc__):
    """Parse and return command-line arguments"""
    parser = ArgumentParser(description=description)
    parser.add_argument(
        'config', type=str, required=False,
        default='/storage/home/pde3/openQ/config.ini',
        help='''Path to config file.'''
    )
    args = parser.parse_args()
    return args


def main():
    """Main"""
    args = parse_args()
    daemon = Daemon(configfile=args.config)
    daemon.serve_forever()


if __name__ == '__main__':
    main()
