#!/usr/bin/env python


"""
Run qstat -x (XML output), cache the output to memory/disk, and parse the
output into Python datastructures.
"""


from __future__ import absolute_import

from argparse import ArgumentParser
from collections import OrderedDict
from datetime import timedelta
from getpass import getuser
from gzip import GzipFile
from numbers import Number
from os import makedirs
from os.path import getmtime, join, isdir, isfile
import re
from subprocess import check_output
from time import time
from xml.etree import ElementTree

from utils import (expand, wstdout, get_xml_subnode, get_xml_val,
                   hhmmss_to_timedelta, to_bool, to_int,
                   sec_since_epoch_to_datetime, to_bytes_size)


__all__ = ['ARRAY_RE', 'CYBERLAMP_QUEUES', 'ACI_QUEUES', 'SORT_COLS', 'Qstat',
           'parse_args', 'main']


ARRAY_RE = re.compile(r'(?P<body>.*)\.(?P<index>\d+)$')

CYBERLAMP_QUEUES = [
    'default', 'cl_open', 'cl_gpu', 'cl_higpu', 'cl_himem', 'cl_debug',
    'cl_phi'
]

ACI_QUEUES = [
    'dfc13_a_g_sc_default', 'dfc13_a_t_bc_default', 'open'
]

SORT_COLS = ['cluster', 'queue', 'job_state', 'job_id']


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
    def __init__(self, stale_sec, cache_dir=None):
        assert isinstance(stale_sec, Number)
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
        self._jobs_df = None

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
            self._jobs_df = None
        return self._jobs

    @property
    def jobs_df(self):
        """pandas.Dataframe : records of jobs' qstat reports"""
        import pandas as pd
        #from pd_utils import convert_df_dtypes

        if self._jobs_df is None:
            jdf = pd.DataFrame(self.jobs)
            self._jobs_df = jdf
            if len(jdf) == 0: # pylint: disable=len-as-condition
                return self._jobs_df

            jdf.sort_values(
                [c for c in SORT_COLS if c in jdf.columns],
                inplace=True
            )

            # Manually convert dtypes of columns that auto convert can't figure
            # out (usually since first element might be `None` or `np.nan`
            if 'interactive' in jdf:
                jdf['interactive'] = jdf['interactive'].astype('category')
            if 'exit_status' in jdf:
                jdf['exit_status'] = jdf['exit_status'].astype('category')
            if 'qos' in jdf:
                jdf['qos'] = jdf['qos'].astype('category')
            if 'req_qos' in jdf:
                jdf['req_qos'] = jdf['req_qos'].astype('category')
            if 'exec_host' in jdf:
                jdf['exec_host'] = jdf['exec_host'].astype('category')

            # Auto-convert dtypes for the remaining columns
            #convert_df_dtypes(jdf)

        return self._jobs_df

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

    def print_summary(self):
        """Display summary info about jobs. """
        label_width = 12
        number_width = 9
        field_widths = (-label_width, -label_width,
                        number_width, number_width, number_width)
        fmt = '  '.join('%' + str(s) + 's' for s in field_widths) + '\n'
        wstdout(fmt % ('Cluster', 'Queue Name', 'Running', 'Queued',
                       'Run+Queue'))
        wstdout(fmt % tuple('-'*int(abs(s)) for s in field_widths))
        total_r = 0
        total_q = 0
        if 'cluster' not in self.jobs_df:
            wstdout(fmt % ('Totals:', '', total_r, total_q, total_r + total_q))
            return

        for cluster, cgrp in self.jobs_df.groupby('cluster'):
            subtot_ser = cgrp.groupby('job_state')['job_state'].count()
            subtot = OrderedDict()
            subtot['R'] = subtot_ser.get('R', default=0)
            subtot['Q'] = subtot_ser.get('Q', default=0)
            total_r += subtot['R']
            total_q += subtot['Q']
            queue_num = 0
            for queue, qgrp in cgrp.groupby('queue'):
                if len(qgrp) == 0: # pylint: disable=len-as-condition
                    continue
                queue_num += 1
                counts = qgrp.groupby('job_state')['job_state'].count()
                if queue_num == 1:
                    cl = cluster
                else:
                    cl = ''
                if len(queue) > label_width:
                    qn = queue[:label_width-3] + '...'
                else:
                    qn = queue

                q_counts = OrderedDict()
                q_counts['R'] = counts.get('R', default=0)
                q_counts['Q'] = counts.get('Q', default=0)

                wstdout(fmt % (cl, qn, q_counts['R'], q_counts['Q'],
                               q_counts['R'] + q_counts['Q']))
            if queue_num > 1:
                wstdout(fmt % ('',
                               '> Subtotals:'.rjust(label_width),
                               subtot['R'],
                               subtot['Q'],
                               subtot['R']+subtot['Q']))
            wstdout('\n')

        wstdout(fmt % ('Totals:', '', total_r, total_q, total_r + total_q))


def parse_args(description=__doc__):
    """parse command-line args"""
    parser = ArgumentParser(description=description)
    parser.add_argument(
        '--stale-sec', type=int, default=60,
        help='''Seconds before cached qstat output is deemed stale. Default is
        60 seconds.'''
    )
    parser.add_argument(
        '--cache-dir', type=str, default=None,
        help='''Directory into which to cache output of qstat. Omit for no
        caching to disk.'''
    )
    args = parser.parse_args()
    return args


def main():
    """Main"""
    args = parse_args()
    qstat = Qstat(**vars(args))
    qstat.print_summary()


if __name__ == '__main__':
    main()
