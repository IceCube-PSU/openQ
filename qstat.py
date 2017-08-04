#!/usr/bin/env python


"""
Command-line qstat-like command with more useful output for our purposes
"""


from __future__ import absolute_import

from argparse import ArgumentParser
from collections import OrderedDict
from ConfigParser import ConfigParser
from getpass import getuser
import os
from os.path import abspath, dirname
from os.path import isfile

import pandas as pd

if __name__ == '__main__' and __package__ is None:
    os.sys.path.append(dirname(dirname(abspath(__file__))))
from openQ import DEFAULT_CONFIG # pylint: disable=wrong-import-position
from openQ.qstat_base import QstatBase # pylint: disable=wrong-import-position
from openQ.utils import expand, log_exc, wstdout # pylint: disable=wrong-import-position


__all__ = ['DEFAULT_STALE_SEC', 'SORT_COLS', 'Qstat', 'parse_args', 'main']


DEFAULT_STALE_SEC = 60
SORT_COLS = ['cluster', 'queue', 'job_state', 'job_id']


class Qstat(QstatBase):
    """
    Subclass of QstatBase for peforming more detailed analysis of qstat
    output. Includes `single_user_summary` function which serves similarly to
    running `qstat` from the command line. See QstatBase for args/kwargs.
    """
    @property
    def jobs_df(self):
        """pandas.Dataframe : records of jobs' qstat reports"""
        # Return in-memory copy
        if not self.jobs_df_is_stale:
            #print 'jobs_df from memory'
            return self._jobs_df

        # Load from disk-cache file
        if self.jobs_df_fpath is not None and not self.jobs_df_file_is_stale:
            #print 'jobs_df from cache file'
            try:
                self._jobs_df = pd.read_pickle(self.jobs_df_fpath)
            except Exception:
                log_exc(pre=('Loading jobs_df from cache file %s failed:'
                             % self.jobs_df_fpath))
            else:
                self._jobs_df_mtime = self.jobs_df_file_mtime
                return self._jobs_df

        #print 'jobs_df being parsed from `jobs`'

        # Parse `jobs` -> `jobs_df` afresh
        self._jobs_df = self.make_jobs_dataframe(self.jobs)
        self._jobs_df_mtime = self.jobs_mtime

        #print 'self.jobs_df_fpath:', self.jobs_df_fpath
        if self.jobs_df_fpath is not None:
            #print 'storing jobs_df to cache file at "%s"' % self.jobs_df_fpath
            self._jobs_df.to_pickle(self.jobs_df_fpath)
            self.set_path_metadata(self.jobs_df_fpath,
                                   mtime=self.xml_file_mtime)

        return self._jobs_df

    @staticmethod
    def make_jobs_dataframe(jobs):
        """Convert the `jobs` (list of dictionaries) to a Pandas DataFrame for
        more flexible analysis.

        Parameters
        ----------
        jobs : list of dictionaries

        Returns
        -------
        jobs_df : pandas.DataFrame

        """
        from pd_utils import convert_df_dtypes

        jobs_df = pd.DataFrame(jobs)
        if len(jobs_df) == 0: # pylint: disable=len-as-condition
            return jobs_df

        jobs_df.sort_values(
            [c for c in SORT_COLS if c in jobs_df.columns],
            inplace=True
        )

        # Manually convert dtypes of columns that auto convert can't figure
        # out (usually since first element might be `None` or `np.nan`
        if 'interactive' in jobs_df:
            jobs_df['interactive'] = jobs_df['interactive'].astype('bool')
        if 'exit_status' in jobs_df:
            jobs_df['exit_status'] = jobs_df['exit_status'].astype('category')
        if 'qos' in jobs_df:
            jobs_df['qos'] = jobs_df['qos'].astype('category')
        if 'req_qos' in jobs_df:
            jobs_df['req_qos'] = jobs_df['req_qos'].astype('category')
        if 'exec_host' in jobs_df:
            jobs_df['exec_host'] = jobs_df['exec_host'].astype('category')
        #for key in ['comp_time', 'total_runtime', 'walltime']:
        #    if key in jobs_df:
        #        jobs_df[key] = jobs_df['exec_host'].astype('category')

        # Auto-convert dtypes for the remaining columns
        #convert_df_dtypes(jobs_df)

        return jobs_df

    def single_user_summary(self):
        """Construct summary info about a single user's jobs.

        Returns
        -------
        summary : string

        """
        lines = []

        jobs_df = self.jobs_df
        label_width = 12
        number_width = 9
        field_widths = (-label_width, -label_width,
                        number_width, number_width, number_width)
        fmt = '  '.join('%' + str(s) + 's' for s in field_widths)
        lines.append(fmt % ('Cluster', 'Queue Name', 'Running', 'Queued',
                            'Run+Queue'))
        lines.append(fmt % tuple('-'*int(abs(s)) for s in field_widths))
        total_r = 0
        total_q = 0
        if 'cluster' not in jobs_df:
            lines.append(fmt % ('Totals:', '', total_r, total_q,
                                total_r + total_q))
            return '\n'.join(lines)

        for cluster, cgrp in jobs_df.groupby('cluster'):
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
                    cluster_ = cluster
                else:
                    cluster_ = ''
                if len(queue) > label_width:
                    queue_name = queue[:label_width-3] + '...'
                else:
                    queue_name = queue

                q_counts = OrderedDict()
                q_counts['R'] = counts.get('R', default=0)
                q_counts['Q'] = counts.get('Q', default=0)

                lines.append(fmt % (cluster_, queue_name, q_counts['R'],
                                    q_counts['Q'],
                                    q_counts['R'] + q_counts['Q']))
            if queue_num > 1:
                lines.append(fmt % ('',
                                    '> Subtotals:'.rjust(label_width),
                                    subtot['R'],
                                    subtot['Q'],
                                    subtot['R']+subtot['Q']))
            lines.append('')

        lines.append(fmt % ('Totals:', '', total_r, total_q,
                            total_r + total_q))
        return '\n'.join(lines)


def parse_args(description=__doc__):
    """parse command-line args"""
    default_cache_dir = None
    if isfile(DEFAULT_CONFIG):
        config = ConfigParser()
        config.read(DEFAULT_CONFIG)
        try:
            default_cache_dir = config.get('Logging', 'qstat_cache_dir')
        except KeyError:
            pass

    parser = ArgumentParser(description=description)
    parser.add_argument(
        '--stale-sec', type=str, default=str(DEFAULT_STALE_SEC),
        help='''Seconds before cached qstat output is deemed stale. Default is
        %d seconds.''' % DEFAULT_STALE_SEC
    )
    parser.add_argument(
        '--config', type=str, default=None,
        help='''openQ config file to use (for retrieving openQ users). Default
        is "%s", if it is necessary (e.g. --all is used).''' % DEFAULT_CONFIG
    )

    cache_help = 'Directory into which to cache output of qstat. '
    if default_cache_dir is not None:
        cache_help += (
            '''Specify an empty string ("") in order to disable caching. If
            --cache-dir is not specified, default is "%s".'''
            % default_cache_dir
        )
    else:
        cache_help += 'If --cache-dir is not specified, caching is disabled.'

    parser.add_argument(
        '--cache-dir', type=str, default=default_cache_dir,
        help=cache_help
    )
    parser.add_argument(
        '--group', type=str, default=None,
        help='''Group for changing group ownership/permissions. Specify an
        empty string, i.e. "", in order to disable group modification of the
        produced cache files. If not specified, defaults to group from the
        config file (--config option). (Note that this has no effect if caching
        is disabled.)'''
    )
    parser.add_argument(
        '--users', type=str, default=None, nargs='+',
        help='''Specify specific user(s) to retrieve for. Separate multiple
        users by spaces. Use special string "all" to retrieve info for all
        users in config. If --users is not provided at all, retrieve info for
        the user running the command ($USER).'''
    )
    args = parser.parse_args()

    return args


def main(config, cache_dir=None, stale_sec=float('inf'), users=None,
         group=None):
    """Main function.

    Parameters
    ----------
    config
    cache_dir
    stale_sec
    users
    group

    Returns
    -------
    jobs_df

    """
    # Convert empty strings into None, else keep a non-empty string
    cache_dir = cache_dir or None
    group = group or None

    myusername = getuser()

    config_ = None
    if users is not None and 'all' in users:
        assert len(users) == 1
        config_ = ConfigParser()
        if config is None:
            config = DEFAULT_CONFIG
        config_.read(expand(config))
        if not config_.sections():
            raise ValueError('No or invalid config. %s' % config)
        users = sorted(config_.get('Users', 'list').split(','))
    elif users is not None:
        users = sorted(users)
    else:
        users = [myusername]

    if group is None and config_ is not None:
        group = config_.get('Users', 'group')

    if len(users) == 1:
        username = users[0]
        if username != myusername:
            stale_sec = 'inf'
        qstat = Qstat(stale_sec=float(stale_sec), username=username,
                      cache_dir=cache_dir, group=group)
        wstdout(qstat.single_user_summary() + '\n')
        return qstat.jobs_df

    jobs_dfs = []
    for user in users:
        #print 'user:', user
        if user == myusername:
            stale_sec_ = float(stale_sec)
        else:
            stale_sec_ = float('inf')

        qstat = Qstat(stale_sec=stale_sec_, username=user, cache_dir=cache_dir,
                      group=group)

        # Load the dataframe, if possible
        try:
            jobs_df = qstat.jobs_df
        except ValueError:
            log_exc(pre='Getting jobs_df for user %s failed:' % user)
            continue

        # Attach username column to the dataframe
        jobs_df['username'] = user

        # Append it to the list
        jobs_dfs.append(jobs_df)

    #print jobs_dfs

    # Concatenate all dataframes to a single dataframe
    jobs_df = pd.concat(jobs_dfs, ignore_index=True, copy=False)

    return jobs_df


if __name__ == '__main__':
    RETVAL = main(**vars(parse_args()))
