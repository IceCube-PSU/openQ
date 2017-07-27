#!/usr/bin/env python


"""
Run qstat -x (XML output), cache the output to memory/disk, and parse the
output into Python datastructures.
"""


from __future__ import absolute_import

from collections import OrderedDict
import cPickle as pickle
from datetime import timedelta
from getpass import getuser
from grp import getgrgid, getgrnam
from gzip import GzipFile
from numbers import Number
from os import chmod, chown, utime
from os.path import getmtime, join, isdir, isfile
import re
from subprocess import check_output
from time import sleep, time
from xml.etree import ElementTree

from utils import (expand, get_xml_subnode, get_xml_val, hhmmss_to_timedelta,
                   mkdir, to_bool, to_int, sec_since_epoch_to_datetime,
                   to_bytes_size)


__all__ = ['ARRAY_RE', 'CYBERLAMP_QUEUES', 'ACI_QUEUES', 'QstatBase']


ARRAY_RE = re.compile(r'(?P<body>.*)\.(?P<index>\d+)$')

CYBERLAMP_QUEUES = [
    'default', 'cl_open', 'cl_gpu', 'cl_higpu', 'cl_himem', 'cl_debug',
    'cl_phi'
]

ACI_QUEUES = [
    'dfc13_a_g_sc_default', 'dfc13_a_t_bc_default', 'open'
]

MAX_ATTEMPTS = 15


class QstatBase(object):
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

    group : None, string, or int
        If None, do not set group or change default permissions on cached
        files. If string or int, interpret as group name or GID, respectively.
        In the latter case, change group ownership of the cached file(s)
        accordingly and set group read/write permissions on the file.

    """
    def __init__(self, stale_sec, cache_dir=None, group=None):
        assert isinstance(stale_sec, Number)
        self.stale_sec = stale_sec
        if isinstance(cache_dir, basestring):
            cache_dir = expand(cache_dir)
        else:
            assert cache_dir is None
        self.cache_dir = cache_dir
        self.myusername = getuser()
        self.gid = None
        self.group = None
        if self.cache_dir is not None:
            if isinstance(group, int):
                self.gid = group
                self.group = getgrgid(self.gid).gr_name
            elif isinstance(group, basestring):
                self.group = group
                self.gid = getgrnam(self.group).gr_gid
            else:
                assert group is None, str(group)
        self._xml = None
        self._jobs = None
        self._jobs_df = None

        self._xml_mtime = float('-inf')
        self._jobs_mtime = float('-inf')
        self._jobs_df_mtime = float('-inf')

        self._xml_file_mtime = float('-inf')
        self._jobs_file_mtime = float('-inf')
        self._jobs_df_file_mtime = float('-inf')

        self.xml_fpath = None
        self.jobs_fpath = None
        self.jobs_df_fpath = None
        if self.cache_dir is not None:
            self.xml_fpath = join(
                self.cache_dir,
                'qstat.%s.xml.gz' % self.myusername
            )
            self.jobs_fpath = join(
                self.cache_dir,
                'jobs.%s.pkl' % self.myusername
            )
            self.jobs_df_fpath = join(
                self.cache_dir,
                'jobs_df.%s.pkl.gz' % self.myusername
            )

        if not isdir(self.cache_dir):
            mkdir(self.cache_dir, perms=0o770, group=self.gid)

    def set_file_metadata(self, fpath, mtime=None):
        """Set group, appropriate permissions, and optionally mtime on a
        filepath. If `self.group` is not None, change the file's group
        ownership to `self.group` and add read+write permissions on the file.

        Parameters
        ----------
        fpath : string
            Full file path

        mtime : None or float
            Seconds since the Unix epoch. If None, no modification to the
            file's mtime is made.

        """
        if self.group is not None:
            chown(fpath, -1, self.gid)
            chmod(fpath, 0o660)

        if mtime is not None:
            access_time = time()
            utime(fpath, (access_time, mtime))

    @property
    def xml_mtime(self):
        """float : seconds since epoch; -inf if no cache or file not found"""
        if self._xml is None:
            self._xml_mtime = float('-inf')
        return self._xml_mtime

    @property
    def jobs_mtime(self):
        """float : seconds since epoch; -inf if no cache or file not found"""
        if self._jobs is None:
            self._jobs_mtime = float('-inf')
        return self._jobs_mtime

    @property
    def jobs_df_mtime(self):
        """float : seconds since epoch; -inf if no cache or file not found"""
        if self._jobs_df is None:
            self._jobs_df_mtime = float('-inf')
        return self._jobs_df_mtime

    @property
    def xml_file_mtime(self):
        """float : seconds since epoch; -inf if no cache or file not found"""
        self._xml_file_mtime = float('-inf')
        if self.xml_fpath is not None and isfile(self.xml_fpath):
            self._xml_file_mtime = getmtime(self.xml_fpath)
        return self._xml_file_mtime

    @property
    def jobs_file_mtime(self):
        """float : seconds since epoch; -inf if no cache or file not found"""
        self._jobs_file_mtime = float('-inf')
        if self.jobs_fpath is not None and isfile(self.jobs_fpath):
            self._jobs_file_mtime = getmtime(self.jobs_fpath)
        return self._jobs_file_mtime

    @property
    def jobs_df_file_mtime(self):
        """float : seconds since epoch; -inf if no cache or file not found"""
        self._jobs_file_mtime = float('-inf')
        if self.jobs_df_fpath is not None and isfile(self.jobs_df_fpath):
            self._jobs_df_file_mtime = getmtime(self.jobs_df_fpath)
        return self._jobs_df_file_mtime

    @property
    def xml_is_stale(self):
        """bool : whether data from qstat is stale and needs to be refreshed"""
        return time() >= self.xml_mtime + self.stale_sec

    @property
    def jobs_is_stale(self):
        return time() >= self.jobs_mtime + self.stale_sec

    @property
    def jobs_df_is_stale(self):
        return time() >= self.jobs_df_mtime + self.stale_sec

    @property
    def xml_file_is_stale(self):
        return time() >= self.xml_file_mtime + self.stale_sec

    @property
    def jobs_file_is_stale(self):
        jf_is_stale = time() >= self.jobs_file_mtime + self.stale_sec
        return self.xml_file_is_stale or jf_is_stale

    @property
    def jobs_df_file_is_stale(self):
        jfdf_is_stale = time() >= self.jobs_df_file_mtime + self.stale_sec
        return self.jobs_file_is_stale or jfdf_is_stale

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
        # If we've already loaded qstat's output at some point, see if it is
        # not stale and return in-memory copy
        if not self.xml_is_stale:
            print 'xml from memory'
            return self._xml

        # Anything besides the above means we'll have to re-parse the xml to
        # extract Pythonic jobs info, so invalidate self._jobs
        self._jobs = None

        # Check if cache file exists and load if not stale
        if not self.xml_file_is_stale:
            print 'xml from cache file'
            try:
                with GzipFile(self.xml_fpath, mode='r') as fobj:
                    self._xml = fobj.read()
            except Exception:
                pass
            else:
                self._xml_mtime = self.xml_file_mtime
                return self._xml

        print 'xml from fresh invocation of qstat'

        # Otherwise, run qstat again
        self._xml = check_output(['qstat', '-x'])
        self._xml_mtime = time()

        # Update the cache file
        if self.xml_fpath is not None:
            with GzipFile(self.xml_fpath, mode='w') as fobj:
                fobj.write(self._xml)
            self.set_file_metadata(self.xml_fpath)

        return self._xml

    @property
    def jobs(self):
        """list of OrderedDict : records of each job qstat reports"""
        # Return in-memory copy
        if not self.jobs_is_stale:
            print 'jobs from memory'
            return self._jobs

        # Load from cache file
        if not self.jobs_file_is_stale:
            print 'jobs from cache file'
            self._jobs = pickle.load(open(self.jobs_fpath, 'rb'))
            self._jobs_mtime = self.jobs_file_mtime
            return self._jobs

        print 'jobs re-parsed from xml'

        # Invalidate the dataframe (not implemented in this class, but in a
        # subclass Qstat; see `qstat.py`.)
        self._jobs_df = None

        # Attempt to parse multiple times in case the XML was loaded in the
        # middle of being written (which would break the parsing done here)
        attempts = 0
        while True:
            attempts += 1
            try:
                self._jobs = self.parse_xml(self.xml)
            except IOError:
                # Invalidate the XML, since the parse failed
                self._xml = None
                if attempts >= MAX_ATTEMPTS:
                    raise
                sleep(5)
            else:
                break

        self._jobs_mtime = time()

        # Cache to disk
        if self.jobs_fpath is not None:
            pickle.dump(self._jobs, open(self.jobs_fpath, 'wb'),
                        protocol=pickle.HIGHEST_PROTOCOL)
            self.set_file_metadata(self.jobs_fpath, mtime=self.xml_file_mtime)

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

            for key in ['total_runtime']:
                if rec[key] is not None:
                    rec[key] = timedelta(seconds=float(rec[key]))

            for key in ['walltime']:
                if rec[key] is not None:
                    rec[key] = hhmmss_to_timedelta(rec[key])

            # TODO: 'comp_time' looks like sec since epoch, e.g., 1501000391,
            # but not sure what comp_time means...

            for key in ['start_time', 'ctime', 'etime', 'mtime', 'qtime',
                        'comp_time']:
                if rec[key] is not None:
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
                        res_val = to_bytes_size(res_val)
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
                        res_val = to_bytes_size(res_val)
                    elif 'time' in res_name or res_name in ['cput']:
                        res_val = hhmmss_to_timedelta(res_val)
                    elif res_name == 'nodes':
                        fields = res_val.split(':')
                        rec['req_nodes'] = int(fields[0])
                        for field in fields[1:]:
                            if '=' in field:
                                name, val = field.split('=')
                                rec['req_' + name] = int(val)
                            else:
                                rec['req_' + field] = True
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
                        req_val = to_bytes_size(req_val)
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