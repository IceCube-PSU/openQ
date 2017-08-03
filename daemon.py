#!/usr/bin/env python


"""
Submit job files located in a directory to ACI open queue.
"""


from __future__ import absolute_import

from argparse import ArgumentParser
#import atexit
from ConfigParser import ConfigParser
#from datetime import datetime
from getpass import getuser
import os
from os import (chdir, dup2, fork, getpid, kill, listdir, remove, setgid,
                setsid, umask)
from os.path import abspath, dirname, getmtime, join, isdir, isfile, ismount
from random import randint
from shutil import copy2, copytree, rmtree
import signal
from socket import gethostname
from subprocess import CalledProcessError, check_output, Popen, STDOUT
import sys
from time import sleep, time

if __name__ == '__main__' and __package__ is None:
    os.sys.path.append(dirname(dirname(abspath(__file__))))
from openQ import DEFAULT_CONFIG # pylint: disable=wrong-import-position
from openQ.qstat_base import QstatBase # pylint: disable=wrong-import-position
from openQ.utils import (copy_contents, expand, get_gid, log_exc, mkdir, # pylint: disable=wrong-import-position
                         remove_contents, rename_or_move, set_path_metadata,
                         wstderr, wstdout)


__all__ = ['FROZEN', 'APPLICATION_PATH', 'APPLICATION_DIR',
           'APPLICATION_MTIME', 'SIGNAL_MAP', 'RESPAWN_DELAY_SEC',
           'NO_CONFIG_SHUTDOWN_SEC', 'Daemon', 'parse_args', 'main']


FROZEN = False
APPLICATION_PATH = None
APPLICATION_DIR = None
APPLICATION_MTIME = float('inf')

if getattr(sys, 'frozen', False):
    APPLICATION_PATH = expand(sys.executable)
    FROZEN = True
elif __file__:
    APPLICATION_PATH = expand(__file__)

if APPLICATION_PATH:
    APPLICATION_DIR = dirname(expand(APPLICATION_PATH))
    APPLICATION_MTIME = getmtime(APPLICATION_PATH)

SIGNAL_MAP = {
    k: v for v, k in reversed(sorted(signal.__dict__.items()))
    if v.startswith('SIG') and not v.startswith('SIG_')
}
"""Mapping from signal numbers to their names"""

RESPAWN_DELAY_SEC = 60
RESPAWN_SIGNALS = ['SIGHUP', 'SIGINT', 'SIGQUIT', 'SIGILL', 'SIGTRAP',
                   'SIGABRT', 'SIGTERM']

NO_CONFIG_SHUTDOWN_SEC = 3600
"""Shutdown if a config file is not found after this long"""


class Daemon(object):
    """
    Queue daemon

    Parameters
    ----------
    configfile : string

    """
    def __init__(self, configfile, daemon=True, logfile=None):
        self.cleaned = False
        self.myusername = getuser()
        if logfile is not None:
            logfile = expand(logfile)
        self.logfile = logfile

        self.config_parser = None
        self.config_fpath = expand(configfile)
        self.config_dir = dirname(self.config_fpath)
        self.config_file_mtime = 0
        self.configured = False
        self.config_last_seen = 0

        self.upgrade_kind = None
        self.distfile = join(self.config_dir, 'dist', 'daemon', 'daemon')
        """File to use when upgrading"""

        self.queue_status = {'q': 0, 'r': 0, 'other': 0}
        self.make_daemon = daemon
        self.is_daemon = False
        self.pid_fpath = expand('~/.pid')
        self.stdin = None
        self.stdout = None
        self.stderr = None
        self.pid = getpid()
        self.hostname = gethostname()
        self.qstat = None
        self.reconfigure()

    def reconfigure(self):
        """If configfile has been updated, reconfigure accordingly"""
        # First check if that file was touched
        try:
            config_file_mtime = getmtime(self.config_fpath)
        except OSError, err:
            if self.configured and err.args[0] == 2:
                dt_sec = time() - self.config_last_seen
                if dt_sec > NO_CONFIG_SHUTDOWN_SEC:
                    wstderr('No config file seen in at least %d sec. Shutting'
                            ' down.\n' % dt_sec)
                    sys.exit(1)
                wstderr('Could not find config "%s", returning to normal'
                        ' operation\n' % self.config_fpath)
                return
            raise
        else:
            self.config_last_seen = time()

        if config_file_mtime <= self.config_file_mtime:
            return

        wstderr('Updated/new config detected; reconfiguring...\n')
        self.config_parser = ConfigParser()
        files_read = self.config_parser.read(self.config_fpath)
        if not files_read:
            wstderr('Config file "%s" could not be read... '
                    % self.config_fpath)
            if self.configured:
                wstderr('Returning to normal operation using previously-read'
                        ' config.\n')
            else:
                wstderr('No configuration exists; exiting.\n')
                sys.exit(1)

        self.users = [u.strip() for u in
                      self.config_parser.get('Users', 'list').split(',')]
        if self.myusername not in self.users:
            raise ValueError(
                'User running script "%s" is not in users list %s'
                % self.myusername, self.users
            )

        self.group = self.config_parser.get('Users', 'group')
        self.gid = None
        try:
            self.gid = get_gid(self.group)
        except OSError:
            pass
        self.set_my_umask_gid()

        for key, _ in self.config_parser.items('Directories'):
            self.setup_dir(key)
        self.n_run = int(self.config_parser.get('Queue', 'n_run'))
        self.n_queue = int(self.config_parser.get('Queue', 'n_queue'))
        self.sleep = int(self.config_parser.get('Queue', 'sleep'))
        self.qstat_cache_dir = expand(
            self.config_parser.get('Logging', 'qstat_cache_dir')
        )
        self.mkdir(self.qstat_cache_dir)
        self.qstat = QstatBase(stale_sec=self.sleep - 1,
                               cache_dir=self.qstat_cache_dir,
                               group=self.group)

        if self.config_parser.has_section('Commands'):
            commands = self.config_parser.items('Commands')
            wstderr('Found [Commands] section, with following commands:\n')
            for command in commands:
                wstderr('> %s = "%s"\n' % command) # DEBUG
                if command[0] == 'upgrade':
                    arg = command[1].lower()
                    if arg in ['auto', 'force']:
                        self.upgrade_kind = arg
                    else:
                        self.upgrade_kind = None
                elif command[0] == 'shutdown':
                    if command[1].lower() == 'true':
                        wstderr('Shutdown command issued.\n')
                        sys.exit(0)

        self.config_file_mtime = config_file_mtime
        self.configured = True

    # TODO: do the upgrade ourselves here, don't call the shell script!
    def upgrade(self):
        """Upgrade the software, if called to do so."""
        if not self.upgrade_kind:
            return

        if self.upgrade_kind not in ['auto', 'force']:
            wstderr('Invalid `upgrade_kind`: %s\n' % self.upgrade_kind)
            wstderr('Continuing to operate without attempting to upgrade.\n')
            return

        if not FROZEN:
            wstderr('Refusing to attempt to upgrade non-frozen application'
                    ' (i.e., this is a source-code distribution).\n')
            return

        if not isfile(self.distfile):
            wstderr('Could not find distfile at path "%s"; not upgrading'
                    ' and resuming normal operation.\n' % self.distfile)
            return

        if self.upgrade_kind == 'auto':
            try:
                dist_mtime = getmtime(self.distfile)
            except OSError:
                wstderr('Could not find distfile at path "%s"; not upgrading'
                        ' and resuming normal operation.\n' % self.distfile)
                return
            if dist_mtime <= APPLICATION_MTIME:
                #wstderr('Distribution `daemon` "%s" not newer than current'
                #        ' running `daemon`; not upgrading.\n' % self.distfile)
                return
            wstderr('Found newer version of the software!\n')

        if APPLICATION_DIR == expand(dirname(self.distfile)):
            wstderr('Application is running out of the dist dir: "%s"; cannot'
                    ' upgrade, resuming normal operation!\n' % APPLICATION_DIR)
            return

        wstderr('Attempting to upgrade the software...\n')

        wstderr('1. Backing up current version of software...\n')

        backupdir = APPLICATION_DIR + '.bak'
        backup_pid_fpath = expand('~/.pid.bak')

        if ismount(backupdir):
            wstderr('Backup dir "%s" is a mount point; refusing to modify.\n'
                    % backupdir)
            wstderr('Cointinuing operation without upgrading!\n')
            return
        elif isdir(backupdir):
            wstderr('Backup dir "%s" already exists; removing.\n' % backupdir)
            rmtree(backupdir)
        elif isfile(backupdir):
            wstderr('Backup dir path "%s" is an existing file; removing.\n'
                    % backupdir)
            remove(backupdir)

        # Backup the current files
        copytree(APPLICATION_DIR, backupdir)
        rename_or_move(self.pid_fpath, backup_pid_fpath)
        orig_pid_fpath = self.pid_fpath
        self.pid_fpath = None
        set_path_metadata(backup_pid_fpath, group=self.group, perms=0o660)

        wstderr('2. Running `deploy.sh` to get and launch new version...\n')
        wstderr('-'*79 + '\n')
        try:
            out = check_output(join(self.config_dir, 'deploy.sh'))
        except CalledProcessError:
            wstderr('-'*79 + '\n')
            wstderr('> Failed to deploy new software. Reverting code and'
                    ' continuing to run.\n')

            # TODO/NOTE: the following fails due to e.g.:
            # OSError: [Errno 16] Device or resource busy:
            #     '~/.dist/.nfs0000000000e2e11900000ea1'
            # but we want to keep going in this case, so for now ignoring
            # OSError
            remove_contents(APPLICATION_DIR)

            copy_contents(backupdir, APPLICATION_DIR)
            copy2(backup_pid_fpath, orig_pid_fpath)
            rmtree(backupdir)
            remove(backup_pid_fpath)
            self.pid_fpath = orig_pid_fpath
        else:
            wstderr('-'*79 + '\n')
            wstderr('> Upgrade appears to have succeeded; output of'
                    ' deploy.sh:\n%s\n' % out)
            wstderr('> Cleaning up and shutting down obsolete daemon...\n')
            rmtree(backupdir)
            remove(backup_pid_fpath)
            sys.exit(0)

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
        basedir = self.config_parser.get('Directories', 'basedir')
        basedir = expand(basedir.replace('<!User!>', usr))
        if dir_kind == 'basedir':
            return basedir
        return join(basedir, self.config_parser.get('Directories', dir_kind))

    def mkdir(self, path):
        """Create a directory and set ownership and permissions appropriately.

        Parameters
        ----------
        path : string
            Directory path to be created

        """
        mkdir(path, perms=0o2770, group=self.group)

    def setup_dir(self, dir_kind):
        """Create dir of a particular kind"""
        path = self.getpath(dir_kind=dir_kind, usr=self.myusername)
        self.mkdir(path)

    def cleanup(self):
        """Cleanup open file descriptors and remove PID file"""
        if self.cleaned:
            return
        wstderr('Cleaning up open file descriptors and removing PID file at'
                ' "%s"\n' % self.pid_fpath)
        if self.pid_fpath is not None:
            remove(self.pid_fpath)
        for fobj in [self.stdin, self.stdout, self.stderr]:
            if fobj is not None:
                fobj.close()
        self.cleaned = True

    def sig_handler(self, signum, frame): # pylint: disable=unused-argument
        """Handle signals"""
        signame = SIGNAL_MAP[signum]
        wstderr('Received signal %d (%s)\n' % (signum, signame))
        self.cleanup()
        # `kill <pid>` sends SIGTERM (`kill -9` sends SIGKILL, but we can't
        # catch this... and we want _some_ way to be able to kill without
        # respawning, so `kill -9` is the way to do this, for now)
        if signame in RESPAWN_SIGNALS:
            Popen(['nohup',
                   join(self.config_dir, 'deploy.sh'),
                   str(RESPAWN_DELAY_SEC)])
        sys.exit(signum)

    @property
    def full(self):
        """bool : Is queue full?"""
        queued = 0
        running = 0
        other = 0
        try:
            jobs = self.qstat.jobs
        except Exception as err:
            log_exc(pre='qstat failed with following traceback:')
            return True

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

        self.queue_status['q'] = queued
        self.queue_status['r'] = running
        self.queue_status['o'] = other

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
                wstderr('User "%s" does not have a job_dir setup.\n' % usr)
                continue
            jobs.extend(
                (usr, f) for f in listdir(dirpath) if isfile(join(dirpath, f))
            )

        # Estimate how much work is needed
        if not jobs:
            wstderr('no jobs!\n')
            return
        free = self.n_run - self.queue_status['r']

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
            if self.qsub(usr=usr, job=job):
                submitted += 1
            else:
                break

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
        #ts_now = timestamp(datetime.now())
        job_dir = self.getpath(dir_kind='job', usr=usr)
        tmp_dir = self.getpath(dir_kind='tmp', usr=usr)
        orig_job_filepath = join(job_dir, job)
        tmp_job_filepath = join(tmp_dir, job)
        try:
            rename_or_move(orig_job_filepath, tmp_job_filepath)
        except (IOError, OSError):
            wstderr('Could not move "%s" to "%s"; moving on.\n'
                    % (orig_job_filepath, tmp_job_filepath))
            return False
        else:
            set_path_metadata(tmp_job_filepath, group=self.group, perms=0o660)

        submitted_dir = self.getpath(dir_kind='sub', usr=usr)
        submitted_filepath = join(submitted_dir, job)

        # TODO: uncomment when we get actual stderr from command
        #qsub_info_dir = self.getpath(dir_kind='qsub_info', usr=usr)
        #suffix = '.qsub.%s.%s' % (self.myusername, ts_now)
        #qsub_err_filepath = join(qsub_info_dir, job + suffix + '.err')
        # TODO: remove altogether?
        #qsub_out_filepath = join(qsub_info_dir, job + suffix + '.out')

        dest_filepath = submitted_filepath
        try:
            wstderr('User %s submitting job %s created by %s\n'
                    % (self.myusername, job, usr))

            # TODO: figure out "-M <email>" option and place here... but might
            # want to check that this option isn't already specified in the
            # file before overrriding...

            # Note that "-m p" disables all email, and "-A open" enqueues the
            # job on the ACI open queue
            mail_options = '-m p'
            qsub_command = 'qsub %s -A open' % mail_options
            out = None
            # TODO: get stderr if command fails; then write to file
            try:
                out = check_output(qsub_command.split() + [tmp_job_filepath],
                                   stderr=STDOUT)
            except CalledProcessError:
                # Want to move job file back to original location, to be
                # processed by another worker
                dest_filepath = orig_job_filepath

                # Report what went wrong with qsub command to stderr and write
                # info to a file
                err_msg = 'Failed to run command "%s %s"\n' % (qsub_command,
                                                               tmp_job_filepath)
                if out is not None:
                    err_msg += (
                        'Output from command:\n'
                        + '\n'.join('> %s\n' % l for l in out.split('\n'))
                    )
                wstderr(err_msg + '\n')
                # TODO uncomment when we can get stderr properly above
                #with open(qsub_err_filepath, 'w') as fobj:
                #    fobj.write(err_msg)
                #set_path_metadata(qsub_err_filepath, group=self.group,
                #                  perms=0o660)
                return False

            # TODO remove this altogether? No need to see stdout if it succeeds
            ## Write qsub message(s) to file (esp. what job_id got # assigned)
            #with open(qsub_out_filepath, 'w') as fobj:
            #    fobj.write(out)
            #set_path_metadata(qsub_out_filepath, group=self.group,
            #                  perms=0o660)
            return True

        finally:
            try:
                rename_or_move(tmp_job_filepath, dest_filepath)
            except (IOError, OSError):
                wstderr('WARNING: Could not move "%s" to "%s"' %
                        (tmp_job_filepath, dest_filepath))
            else:
                set_path_metadata(dest_filepath, group=self.group,
                                  perms=0o660)

    def kill_existing_daemon(self):
        """If an existing daemon is found on the same host, attempt to kill it.

        Raises
        ------
        OSError
            If existing process exists but cannot be killed.

        """
        if self.pid_fpath is None or not isfile(self.pid_fpath):
            return

        with open(self.pid_fpath, 'r') as fobj:
            lines = fobj.readlines()
        pid_in_file = int(lines[-2].strip())
        host_in_file = lines[-1].strip()

        if pid_in_file == self.pid:
            return

        if host_in_file == self.hostname:
            wstderr('Attempting to kill process %d on host %s ... '
                    % (pid_in_file, host_in_file))
            try:
                kill(pid_in_file, signal.SIGKILL)
            except OSError as err:
                if err.args[0] == 3:
                    wstderr('[SUCCESS] (Process did not exist.)\n')
                    remove(self.pid_fpath)
                    return
                wstderr('[FAILURE] (Could not kill process with SIGTERM.)\n')
                raise
            else:
                wstderr('[SUCCESS] (Process killed with SIGTERM.)\n')
                remove(self.pid_fpath)
                return

        #with open(expand('~/.openQ.err'), 'w') as fobj:
        #    fobj.write('host_in_file = %s, self.hostname = %s\n'
        #               % (host_in_file, self.hostname))
        #set_path_metadata(expand('~/.openQ.err'), perms=0o660,
        #                  group=self.group)

        raise OSError(
            'PID file already exists at "%s"! Kill possible existing'
            ' daemon with PID %d on host %s (attempt with kill before'
            ' kill -9). If process is already dead, remove the PID file'
            ' and then attempt to run & daemonize again.'
            % (self.pid_fpath, pid_in_file, host_in_file)
        )

    def set_my_umask_gid(self):
        """Set umask and--if one is defined--group ID (GID)"""
        # Turn on user and group, disable "other" permissions
        umask(0)
        # TODO: the following fails. Any way to setgid on process?
        if self.gid is not None:
            try:
                setgid(self.gid)
            except OSError:
                pass

    def daemonize(self):
        """See https://gist.github.com/josephernest/77fdb0012b72ebdf4c9d19d6256a1119"""
        if self.is_daemon:
            return

        #self.kill_existing_daemon()

        # First fork
        try:
            pid = fork()
            if pid > 0:
                # Exit first parent
                sys.exit(0)
        except OSError as err:
            wstderr('Fork #1 failed: %d (%s)\n' % (err.args[0], err.args[1]))
            sys.exit(1)

        # Decouple from parent environment
        # NOTE: changing to different dir might mess with pyinstaller...
        # need to make sure that "path" still thinks it lives elsewhere...
        chdir('/')
        setsid()
        self.set_my_umask_gid()

        # Second fork
        try:
            pid = fork()
            if pid > 0:
                # Exit from second parent
                sys.exit(0)
        except OSError as err:
            wstderr('Fork #2 failed: %d (%s)\n' % (err.args[0], err.args[1]))
            sys.exit(1)

        chdir('/')
        self.set_my_umask_gid()

        self.pid = getpid()
        wstdout('%d\n' % self.pid)
        self.is_daemon = True

        # Redirect IO from terminal to either a file or /dev/null (the latter
        # for a proper daemon)
        sys.stdout.flush()
        sys.stderr.flush()
        self.stdin = open(name='/dev/null', mode='r')
        if self.logfile is not None:
            log_fpath = self.logfile
            self.stdout = open(name=log_fpath, mode='w')
            self.stderr = open(name=log_fpath, mode='w', buffering=0)
            set_path_metadata(self.logfile, group=self.group, perms=0o660)
        else:
            log_fpath = '/dev/null'
            self.stdout = open(name=log_fpath, mode='a+')
            self.stderr = open(name=log_fpath, mode='a+', buffering=0)
        dup2(self.stdin.fileno(), sys.stdin.fileno())
        dup2(self.stdout.fileno(), sys.stdout.fileno())
        dup2(self.stderr.fileno(), sys.stderr.fileno())

        # Signal handling
        #atexit.register(self.cleanup)
        for sig in [signal.SIGTERM, signal.SIGHUP, signal.SIGINT]:
            signal.signal(sig, self.sig_handler)

        self.record_pid()

    def record_pid(self):
        """Write PID and host to ~/.pid file"""
        if self.pid_fpath is not None:
            with open(self.pid_fpath, 'w') as pid_file:
                pid_file.write('%s\n%s\n' % (self.pid, self.hostname))
            set_path_metadata(self.pid_fpath, group=self.group, perms=0o660)

    def serve_forever(self):
        """Main loop"""
        self.kill_existing_daemon()
        self.record_pid()

        self.reconfigure()
        self.upgrade()

        if self.make_daemon:
            self.daemonize()

        while True:
            try:
                if self.full:
                    wstderr('Queue is full (or qstat failed).\n')
                else:
                    self.do_some_work()

                wstderr('Going to sleep for %s seconds...\n' % self.sleep)
                sleep(self.sleep)

                self.reconfigure()
                self.upgrade()
            except:
                self.cleanup()
                raise


def parse_args(description=__doc__):
    """Parse and return command-line arguments"""
    parser = ArgumentParser(description=description)
    parser.add_argument(
        '--config', type=str, default=DEFAULT_CONFIG,
        help='''Path to config file.'''
    )
    parser.add_argument(
        '--not-daemon', action='store_true',
        help='''Do *not* deamonize process (i.e., run interactively).'''
    )
    parser.add_argument(
        '--logfile', default=None, type=str,
        help='''Write stdout and stderr to this logfile'''
    )
    args = parser.parse_args()
    return args


def main():
    """Main"""
    args = parse_args()
    daemon = Daemon(configfile=args.config, daemon=(not args.not_daemon),
                    logfile=args.logfile)
    daemon.serve_forever()


if __name__ == '__main__':
    main()
