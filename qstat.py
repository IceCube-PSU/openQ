#!/usr/bin/env python


"""
Command-line qstat-like command with more useful output for our purposes
"""


from __future__ import absolute_import

from argparse import ArgumentParser
from collections import OrderedDict

import pandas as pd

from qstat_base import QstatBase
from utils import wstdout


__all__ = ['SORT_COLS', 'Qstat', 'parse_args', 'main']


SORT_COLS = ['cluster', 'queue', 'job_state', 'job_id']


class Qstat(QstatBase):
    """
    Subclass of QstatBase for peforming more detailed analysis of qstat
    output. Includes `print_summary` function which serves similarly to running
    `qstat` from the command line. See QstatBase for args/kwargs.
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
            self._jobs_df = pd.read_pickle(self.jobs_df_fpath)
            self._jobs_df_mtime = self.jobs_df_file_mtime
            return self._jobs_df

        #print 'jobs_df being parsed from `jobs`'

        # Parse `jobs` -> `jobs_df` afresh
        self._jobs_df = self.make_jobs_dataframe(self.jobs)
        self._jobs_df_mtime = self.jobs_mtime

        if self.jobs_df_fpath is not None:
            self._jobs_df.to_pickle(self.jobs_df_fpath)
            self.set_file_metadata(self.jobs_df_fpath,
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
        #from pd_utils import convert_df_dtypes

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
            jobs_df['interactive'] = jobs_df['interactive'].astype('category')
        if 'exit_status' in jobs_df:
            jobs_df['exit_status'] = jobs_df['exit_status'].astype('category')
        if 'qos' in jobs_df:
            jobs_df['qos'] = jobs_df['qos'].astype('category')
        if 'req_qos' in jobs_df:
            jobs_df['req_qos'] = jobs_df['req_qos'].astype('category')
        if 'exec_host' in jobs_df:
            jobs_df['exec_host'] = jobs_df['exec_host'].astype('category')

        # Auto-convert dtypes for the remaining columns
        #convert_df_dtypes(jobs_df)

        return jobs_df

    def print_summary(self):
        """Display summary info about jobs. """
        jobs_df = self.jobs_df
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
        if 'cluster' not in jobs_df:
            wstdout(fmt % ('Totals:', '', total_r, total_q, total_r + total_q))
            return

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

                wstdout(fmt % (cluster_, queue_name, q_counts['R'],
                               q_counts['Q'],
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
        '--stale-sec', type=int, default=120,
        help='''Seconds before cached qstat output is deemed stale. Default is
        60 seconds.'''
    )
    parser.add_argument(
        '--cache-dir', type=str, default='/gpfs/group/dfc13/default/qstat_out',
        help='''Directory into which to cache output of qstat. Specify an empty
        string, i.e. "", in order to disable caching.'''
    )
    parser.add_argument(
        '--group', type=str, default='dfc13_collab',
        help='''Group for changing group ownership/permissions. Specify an
        empty string, i.e. "", in order to disable group modification of the
        produced cache files. (This has no effect if caching is disabled.)'''
    )
    args = parser.parse_args()

    # Convert empty strings into None, else keep a non-empty string
    args.cache_dir = args.cache_dir or None
    args.group = args.group or None

    return args


def main():
    """Main"""
    args = parse_args()
    qstat = Qstat(**vars(args))
    qstat.print_summary()


if __name__ == '__main__':
    main()
