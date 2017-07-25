#!/usr/bin/env python


"""
Submit job files to open queue
"""


from __future__ import absolute_import

from collections import OrderedDict
import ConfigParser
from datetime import datetime, timedelta, tzinfo
from getpass import getuser
from gzip import GzipFile
from math import ceil
from os import makedirs, chown, chmod, listdir, rename
from os.path import expanduser, expandvars, getmtime, join, isdir, isfile
from random import randint
import re
import stat
from subprocess import CalledProcessError, check_output, STDOUT
import sys
from time import (sleep, time, timezone, altzone, daylight, tzname, mktime,
                  localtime)
from xml.etree import ElementTree


__all__ = ['ARRAY_RE', 'CYBERLAMP_QUEUES', 'ACI_QUEUES', 'UTCTimezone',
           'TZ_UTC', 'LocalTimezone', 'TZ_LOCAL', 'expand', 'wstdout',
           'wstderr', 'get_xml_subnode', 'get_xml_val', 'hhmmss_to_timedelta',
           'to_bool', 'to_int', 'sec_since_epoch_to_datetime', 'to_bool',
           'Daemon', 'Qstat']


ARRAY_RE = re.compile(r'(?P<body>.*)\.(?P<index>\d+)$')
CYBERLAMP_QUEUES = [
    'default', 'cl_open', 'cl_gpu', 'cl_higpu', 'cl_himem', 'cl_debug',
    'cl_phi'
]
ACI_QUEUES = [
    'dfc13_a_g_sc_default', 'dfc13_a_t_bc_default', 'open'
]

STDOFFSET = timedelta(seconds=-timezone)
if daylight:
    DSTOFFSET = timedelta(seconds=-altzone)
else:
    DSTOFFSET = STDOFFSET
DSTDIFF = DSTOFFSET - STDOFFSET
TD_ZERO = timedelta(0)

class UTCTimezone(tzinfo):
    """UTC"""

    def utcoffset(self, dt): # pylint: disable=unused-argument
        return TD_ZERO

    def tzname(self, dt): # pylint: disable=unused-argument
        return "UTC"

    def dst(self, dt): # pylint: disable=unused-argument
        return TD_ZERO

TZ_UTC = UTCTimezone()

class LocalTimezone(tzinfo):
    """Local time"""

    def utcoffset(self, dt):
        if self._isdst(dt):
            return DSTOFFSET
        return STDOFFSET

    def dst(self, dt):
        if self._isdst(dt):
            return DSTDIFF
        return TD_ZERO

    def tzname(self, dt):
        return tzname[self._isdst(dt)]

    @staticmethod
    def _isdst(dt):
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, 0)
        stamp = mktime(tt)
        tt = localtime(stamp)
        return tt.tm_isdst > 0

TZ_LOCAL = LocalTimezone()


def expand(s):
    """Shortcut to expand path or string"""
    return expanduser(expandvars(s))


def wstdout(msg):
    """Write `msg` to stdout & flush immediately"""
    sys.stdout.write(msg)
    sys.stdout.flush()


def wstderr(msg):
    """Write `msg` to stderr & flush immediately"""
    sys.stderr.write(msg)
    sys.stderr.flush()


def get_xml_subnode(node, key):
    """Get sub-node in XML node by key, i.e. ``<node><subnode>...</subnode>``

    Note that this returns ONLY the first occurrence of `key`.

    Parameters
    ----------
    node : ElementTree node
    key : string

    Returns
    -------
    subnode : ElementTree node or None
        If the node could not be found, None is returned.

    """
    try:
        subnode = next(node.iter(key))
    except StopIteration:
        subnode = None
    return subnode


def get_xml_val(node, key):
    """Get string value in XML node by key, i.e. ``<key>val</key>``

    Note that this returns ONLY the first occurrence of `key`.

    Parameters
    ----------
    node : ElementTree node
    key : string

    Returns
    -------
    val : string

    """
    subnode = get_xml_subnode(node, key)
    if subnode is not None:
        val = subnode.text
    else:
        val = None
    return val


def hhmmss_to_timedelta(t):
    """Convert string like hh:mm:ss to a `datetime.timedelta`
    object. If `t` is None, return None.

    Parameters
    ----------
    t : string or None

    Returns
    -------
    td : None or datetime.timedelta

    """
    if t is None:
        return t
    h, m, s = t.split(':')
    return timedelta(hours=h, minutes=m, seconds=s)


def to_bool(b):
    """Convert string to bool or keep None if None"""
    return b if b is None else bool(b)


def to_int(i):
    """Convert string to int or keep None if None"""
    return i if i is None else int(i)


def sec_since_epoch_to_datetime(s):
    """Convert seconds since epoch into `datetime.datetime` object

    Parameters
    ----------
    s : None or convertible to float

    Returns
    -------
    dt : None or datetime.datetime
        If `s` is None, returns None; otherwise, converts to datetime.datetime

    """
    if s is None:
        return s
    if isinstance(s, basestring):
        s = float(s)
    return datetime.fromtimestamp(s, tz=TZ_UTC).astimezone(TZ_LOCAL)


def to_bytes(s):
    """Convert a qstat size string to int bytes.

    Parameters
    ----------
    s : string

    Returns
    -------
    size : int

    Notes
    -----
    See linux.die.net/man/7/pbs_resources for definition of size spec. Not
    implementing "word" logic here.

    """
    scales = dict(k=1024, m=1024**2, g=1024**3, t=1024**4, p=1024**5,
                  e=1024**6)
    num_re = re.compile(r'(?P<mag>\d+)(?P<scale>[kmgtpe]){0,1}(?:[b]{0,1})',
                        re.IGNORECASE)
    match = num_re.match(s)
    if match is None:
        raise ValueError('Failed to parse quantity "%s"' % s)
    groupdict = num_re.match(s).groupdict()
    if groupdict['scale'] is not None:
        factor = scales[groupdict['scale'].lower()]
    else:
        factor = 1
    return int(ceil(float(groupdict['mag']) * factor))


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
        self.configfile = configfile
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
            self.gid = int(self.config.get('Users', 'gid'))
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
        path = expand(path)
        # makedir if doesn't exist
        if not isdir(path):
            makedirs(path)
        # change group owner
        chown(path, -1, self.gid)
        # give group (and user) permissions
        chmod(path, stat.S_IRWXG | stat.S_IRWXU)

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


class Qstat(object):
    """
    Retrieve, cache, and parse into Pythonic datastructures the qstat XML
    output.

    Parameters
    ----------
    stale_sec : numeric
        Forces at least this much time between invocations of the qctual
        `qstat` command by storing to / reading from the cache

    cache_dir : None or string
        Directory in which to save cached qstat output. If `None`, no caching
        to disk will be performed.

    """
    def __init__(self, stale_sec=60, cache_dir=None):
        self.stale_sec = stale_sec
        if isinstance(cache_dir, basestring):
            cache_dir = expand(cache_dir)
        else:
            assert cache_dir is None
        self.cache_dir = cache_dir
        self.myusername = getuser()
        self._xml = None
        self._xml_mtime = None
        self._jobs = None

    @property
    def xml(self):
        """Get qstat output, caching by saving to a gzipped XML file such that
        repeated calls within `self.sleep` seconds will not invoke `qstat`
        command again.

        Note the cache file, if `cache_dir` is not None, is written to
        ``<self.cache_dir>/qstat.<self.myusername>.xml.gz``

        Returns
        -------
        xml : string
            Output from `qstat -x`

        """
        fpath = None
        if self.cache_dir is not None:
            fpath = join(self.cache_dir, 'qstat.%s.xml.gz' % self.myusername)
            if not isdir(self.cache_dir):
                makedirs(self.cache_dir)

        stale_before = time() - self.stale_sec

        # If we've already loaded qstat's output at some point, see if it is
        # not stale
        if (self._xml is not None and self._xml_mtime is not None
                and self._xml_mtime > stale_before):
            return self._xml

        # Anything besides the above means we'll have to re-parse the xml to
        # extract Pythonic jobs info, so invalidate self._jobs
        self._jobs = None

        # Check if cache file exists and load if not stale
        if fpath is not None and isfile(fpath):
            xml_mtime = getmtime(fpath)
            if xml_mtime > stale_before:
                try:
                    with GzipFile(fpath, mode='r') as f:
                        self._xml = f.read()
                except Exception:
                    pass
                else:
                    self._xml_mtime = xml_mtime
                    return self._xml

        # Otherwise, run qstat again
        self._xml = check_output(['qstat', '-x'])
        self._xml_mtime = time()

        # Update the cache file
        if fpath is not None:
            with GzipFile(fpath, mode='w') as f:
                f.write(self._xml)

        return self._xml

    @property
    def jobs(self):
        """list of OrderedDict : records of each job qstat reports"""
        if self._jobs is None:
            self._jobs = self.parse_xml(self.xml)
        return self._jobs

    @staticmethod
    def parse_xml(xml):
        """Parse qstat XML output into Python datastructures that are easier to
        navigate than the original XML.

        Parameters
        ----------
        xml : string
            The output produced by running `qstat -x`

        Returns
        -------
        jobs : list of 1-level-deep OrderedDicts
            There is one OrderedDict for each job in the listing.

            Some fields are new or modified from the original qstat XML. E.g.
            'cluster' and 'queue' are designed to be succinct and maximally
            useful for our purposes here (i.e., these two items will uniquely
            identify where a job is running). Also 'job_id' is _just_ the
            numerical part of the job ID, whereas 'full_job_id' is the full
            string Job ID (as is originally called Job_ID in the XML).
            [
                {
                    'job_id': <str : full job ID>,
                    'job_name': <str>,
                    'job_owner': <str>,
                    'job_state': <character>,
                    'server': <str>,
                    'queue': <str>,
                    'submit_args': <str>,
                    'submit_host': <str>,
                    'start_time': <str>,
                    'account_name': <str>,
                    'cluster': <str>,
                    .
                    .
                    .
                },
                { ... }, ...
            ]

        """
        level1_keys = [
            'Job_Id', 'Job_Name', 'Job_Owner', 'job_state',
            'server', 'Account_Name', 'queue',
            'submit_args', 'submit_host',
            'interactive', 'exit_status', 'exec_host',
            'init_work_dir',
            'start_time', 'Walltime', 'ctime', 'etime', 'mtime', 'qtime',
            'comp_time', 'total_runtime',
        ]

        jobs = []
        for job in ElementTree.fromstring(xml):
            rec = OrderedDict()
            for key in level1_keys:
                low_key = key.lower()
                val = get_xml_val(job, key)
                rec[low_key] = val

            # Translate a few values to be easier to use/understand...
            rec['job_owner'] = rec['job_owner'].split('@')[0]
            rec['interactive'] = to_bool(rec['interactive'])
            rec['exit_status'] = to_int(rec['exit_status'])

            for key in ['comp_time', 'walltime', 'total_runtime']:
                rec[key] = hhmmss_to_timedelta(rec[key])

            for key in ['start_time', 'ctime', 'etime', 'mtime', 'qtime']:
                rec[key] = sec_since_epoch_to_datetime(rec[key])

            account_name = rec.pop('account_name')
            if account_name == 'cyberlamp':
                rec['cluster'] = 'cyberlamp'
                rec['queue'] = 'default'
            elif account_name in ACI_QUEUES:
                rec['cluster'] = 'aci'
                rec['queue'] = account_name.lower()
            else:
                raise ValueError('Unhandled account_name "%s"' % account_name)

            # Flatten hierarchical values: resources_used and resource_list

            resources_used = get_xml_subnode(job, 'resources_used')
            if resources_used is not None:
                for res in resources_used:
                    res_name = res.tag.lower()
                    res_val = res.text
                    if 'mem' in res_name:
                        res_val = to_bytes(res_val)
                    elif 'time' in res_name or res_name == 'cput':
                        res_val = hhmmss_to_timedelta(res_val)
                    elif res_name == 'energy_used':
                        continue
                    rec['used_' + res_name] = res_val

            resource_list = get_xml_subnode(job, 'Resource_List')
            if resource_list is not None:
                for res in resource_list:
                    res_name = res.tag.lower()
                    res_val = res.text
                    if 'mem' in res_name:
                        res_val = to_bytes(res_val)
                    elif 'time' in res_name or res_name in ['cput']:
                        res_val = hhmmss_to_timedelta(res_val)
                    elif res_name == 'nodes':
                        fields = res_val.split(':')
                        rec['req_nodes'] = int(fields[0])
                        for field in fields[1:]:
                            name, val = field.split('=')
                            rec['req_' + name] = int(val)
                    elif res_name == 'qos':
                        rec['qos'] = res_val
                    rec['req_' + res_name] = res_val

                if rec['server'].endswith('aci.ics.psu.edu'):
                    if rec['cluster'] == 'cyberlamp':
                        qos = get_xml_val(resource_list, 'qos')
                        if qos is None:
                            rec['queue'] = 'default'
                        else:
                            rec['queue'] = qos

            req_information = get_xml_subnode(job, 'req_information')
            if req_information is not None:
                contained_lists = OrderedDict()
                for req in req_information:
                    req_name = req.tag.lower()
                    if req_name.startswith('task_usage'):
                        continue
                    match0 = ARRAY_RE.match(req_name)
                    groupdict = match0.groupdict()
                    req_name = groupdict['body'].replace('.', '_')
                    req_val = req.text
                    if req_name in ['task_count', 'lprocs']:
                        req_val = int(req_val)
                    elif req_name == 'memory':
                        req_val = to_bytes(req_val)
                    if req_name not in contained_lists:
                        contained_lists[req_name] = []
                    contained_lists[req_name].append(req_val)

                for req_name, lst in contained_lists.items():
                    if len(lst) == 1:
                        rec[req_name] = lst[0]
                    else:
                        rec[req_name] = ','.join(str(x) for x in lst)

            jobs.append(rec)

        return jobs


if __name__ == '__main__':
    Daemon('/storage/home/pde3/openQ/config.ini').serve_forever()
