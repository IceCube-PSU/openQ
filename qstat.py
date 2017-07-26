#!/usr/bin/env python


"""
Command-line qstat-like command with more useful output for our purposes
"""


from __future__ import absolute_import

from argparse import ArgumentParser
from collections import OrderedDict
from os.path import join

import pandas as pd

from qstat_base import QstatBase
from utils import wstdout


__all__ = ['SORT_COLS', 'Qstat', 'parse_args', 'main']


SORT_COLS = ['cluster', 'queue', 'job_state', 'job_id']


class Qstat(QstatBase):
    @property
    def jobs_df(self):
        """pandas.Dataframe : records of jobs' qstat reports"""
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
