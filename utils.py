# pylint: disable=round-builtin

"""
Assortment of utility functions and classes
"""


from __future__ import absolute_import, division

from datetime import datetime, timedelta, tzinfo
from grp import getgrgid, getgrnam
from math import ceil
import os
from os import (chmod, chown, getgroups, listdir, makedirs, remove, rename,
                rmdir, stat, utime)
from os.path import abspath, expanduser, expandvars, isdir, join
import re
from shutil import copy2, copytree, move
from sys import stderr, stdout
from time import altzone, daylight, localtime, mktime, time, timezone, tzname
from traceback import format_exc

if __name__ == '__main__' and __package__ is None:
    os.sys.path.append(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )

from openQ import GPU_GROUPS # pylint: disable=wrong-import-position


__all__ = ['expand', 'set_path_metadata', 'wstdout', 'wstderr',
           'log_exc', 'UTCTimezone', 'TZ_UTC', 'LocalTimezone', 'TZ_LOCAL',
           'timestamp', 'get_xml_subnode', 'get_xml_val',
           'ddhhmmss_to_timedelta', 'to_bool', 'to_int',
           'sec_since_epoch_to_datetime', 'to_bytes_size', 'GPUS_RE', 'QOS_RE',
           'parse_pbs_command_file', 'gpu_access']


def expand(path):
    """Shortcut to expand path or string"""
    return abspath(expanduser(expandvars(path)))


def rename_or_move(src, dest):
    """Try to move a file by first using `os.rename` or, if that fails, try
    using `shutil.move`.

    Parameters
    ----------
    src : string
        Source path name

    dest : string
        Destination path name

    """
    try:
        rename(src, dest)
    except (IOError, OSError):
        move(src, dest)


def get_gid(group):
    """Get group ID (GID).

    Parameters
    ----------
    group: string, int, or None

    Returns
    -------
    gid : int

    """
    if isinstance(group, basestring):
        gid = getgrnam(group).gr_gid
    elif isinstance(group, int):
        gid = group
    else:
        if group is not None:
            raise TypeError('Illegal type for `group` ("%s"): %s'
                            % (group, type(group)))
        gid = group
    return gid


def set_path_metadata(path, perms=None, group=None, mtime=None):
    """Set permissions, group, and/or modification time (mtime) on a path.

    Parameters
    ----------
    path : string
        Full file path

    perms
        Permissions

    group : None, int, or string
        If int, interpret as group ID (GID); if string, interpret as group
        name; if None, do not change group on the file.

    mtime : None or float
        Seconds since the Unix epoch. If None, no modification to the
        file's mtime is made.

    """
    path = expand(path)
    gid = None
    try:
        gid = get_gid(group)
    except (KeyError, OSError):
        pass

    # Set permissions
    if perms is not None:
        try:
            chmod(path, perms)
        except (IOError, OSError):
            pass

    # Change group owner (note that -1 keeps user the same)
    if gid is not None and stat(path).st_gid != gid:
        try:
            chown(path, -1, gid)
        except (IOError, OSError):
            pass

    # Change modification time on the file to `mtime`; set access time to now
    if mtime is not None:
        access_time = time()
        try:
            utime(path, (access_time, mtime))
        except (IOError, OSError):
            pass


def mkdir(path, perms=None, group=None):
    """Create a directory and set ownership and permissions appropriately.

    Parameters
    ----------
    path : string
        Directory path to be created

    perms
        Permissions to set; if None, permissions aren't changed

    group : string, int, or None
        If not None, set group Group ID of created directory. If `group` is a
        string, it is interpreted as a group name; if an int, it is interpreted
        as group ID (GID).

    """
    path = expand(path)

    # Make dir if doesn't exist; will error out if path exists but is a file
    # (as desired)
    if not isdir(path):
        makedirs(path)

    set_path_metadata(path=path, perms=perms, group=group)


def copy_contents(srcdir, destdir):
    """Copy the contents of `srcdir` into `destdir`. Ignore errors due to .nfs*
    files refusing to be modified."""
    if not isdir(srcdir):
        raise ValueError('`copy_contents` requires `srcdir` "%s" be a'
                         ' directory, but it is not..' % srcdir)
    if not isdir(destdir):
        raise ValueError('`copy_contents` requires `destdir` "%s" be a'
                         ' directory, but it is not..' % destdir)
    for name in listdir(srcdir):
        source_path = join(srcdir, name)
        if not isdir(source_path):
            try:
                copy2(source_path, destdir)
            except (IOError, OSError) as err:
                if '.nfs' in str(err):
                    wstdout('Ignoring error: %s\n' % err)
                else:
                    raise
        else:
            dest_path = join(destdir, name)
            copytree(source_path, dest_path)


def remove_contents(target):
    """Remove the contents of `target` directory. Ignore errors due to .nfs*
    files refusing to be removed."""
    if not isdir(target):
        raise ValueError('`target` "%s" must be a directory.' % target)

    for name in listdir(target):
        source_path = join(target, name)
        try:
            isdir(source_path)
        except OSError:
            continue

        try:
            if not isdir(source_path):
                remove(source_path)
        except (IOError, OSError) as err:
            if '.nfs' in str(err):
                wstdout('Ignoring error: %s\n' % err)
                continue
            raise
        else:
            continue

        remove_contents(source_path)
        try:
            rmdir(source_path)
        except OSError, err:
            if err.args[0] != 39: # ignore dir not empty
                raise


def wstdout(msg):
    """Write `msg` to stdout & flush immediately"""
    stdout.write(msg)
    stdout.flush()


def wstderr(msg):
    """Write `msg` to stderr & flush immediately"""
    stderr.write(msg)
    stderr.flush()


def log_exc(pre=None, post=None):
    """Log exception (and traceback)"""
    wstderr('-'*79 + '\n')
    s = format_exc()
    lines = []
    if pre is not None:
        lines += pre
    lines += ['> %s' % _ for _ in s.strip().split('\n')]
    if post is not None:
        lines += post
    tb_txt = '\n'.join(lines)
    wstderr(tb_txt + '\n')
    wstderr('-'*79 + '\n')


class UTCTimezone(tzinfo):
    """UTC"""

    def utcoffset(self, dt): # pylint: disable=unused-argument
        return TD_ZERO

    def tzname(self, dt): # pylint: disable=unused-argument
        return "UTC"

    def dst(self, dt): # pylint: disable=unused-argument
        return TD_ZERO

TZ_UTC = UTCTimezone()


STDOFFSET = timedelta(seconds=-timezone)
if daylight:
    DSTOFFSET = timedelta(seconds=-altzone)
else:
    DSTOFFSET = STDOFFSET
DSTDIFF = DSTOFFSET - STDOFFSET
TD_ZERO = timedelta(0)

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
        stamp = mktime((dt.year, dt.month, dt.day,
                        dt.hour, dt.minute, dt.second,
                        dt.weekday(), 0, 0))
        time_tuple = localtime(stamp)
        return time_tuple.tm_isdst > 0

TZ_LOCAL = LocalTimezone()


def timestamp(dt=None, tz=TZ_LOCAL, sec_decimals=0, dt_sep='T', hms_sep=':'):
    """Create a timestamp string.

    Parameters
    ----------
    dt : None or datetime.datetime
        Date/time to use to generate the timestamp. If None, use current
        date/time.

    tz : datetime.tzinfo
        Timezone to use. Defaults to local timezone (`TZ_LOCAL`), but you can
        also specify e.g. `openQ.utils.TZ_UTC` for UTC, or a timezone as
        defined in the `pytz` module.

    sec_decimals : int from 0 to 6
        Decimals to use for seconds. String display will be truncated to this
        many decimals (and if 0, no decimal point will be displayed)

    dt_sep : string
        Separator between date and time. E.g., "T" or " " are common choices.

    hms_sep : string
        Separator to use between hours, minutes, and seconds (including between
        hours and minutes in UTC offset). E.g., ":" is standard, or specify ""
        for no separator.

    Returns
    -------
    stamp : string

    """
    if dt is None:
        dt = datetime.now(tz=tz)
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)

    if float(sec_decimals) != int(sec_decimals):
        raise TypeError('`sec_decimals` must be an integer or convertible to an'
                        ' integer without changing its value. Got %.15e'
                        ' instead.' % sec_decimals)
    sec_decimals = int(sec_decimals)
    precision = int(10**sec_decimals)

    rounded_at_prec = int(
        round((dt.second*1e6 + 701532) / (1e6/precision))
    )
    left_of_dec, right_of_dec = divmod(rounded_at_prec, precision)
    sec_str = '%02d' % left_of_dec
    if sec_decimals > 0:
        sec_str = sec_str + ('.{:0%dd}' % sec_decimals).format(right_of_dec)
    utc_offset = dt.utcoffset()
    total_utc_offset_sec = (utc_offset.days * 3600 * 24) + utc_offset.seconds
    utc_hours, remainder_sec = divmod(total_utc_offset_sec, 3600)
    utc_minutes = int(round(remainder_sec / 60, 0) * 60)
    stamp_fmt = ('{year:04d}-{month:02d}-{day:02d}'
                 '{dt_sep}'
                 '{hour:02d}{hms_sep}{minute:02d}{hms_sep}{sec_str}'
                 '{utc_hours:+03d}{hms_sep}{utc_minutes:02d}')
    stamp = stamp_fmt.format(
        year=dt.year, month=dt.month, day=dt.day, dt_sep=dt_sep,
        hour=dt.hour, minute=dt.minute, utc_hours=utc_hours,
        utc_minutes=utc_minutes, sec_str=sec_str, hms_sep=hms_sep
    )
    return stamp


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


def ddhhmmss_to_timedelta(time_string):
    """Convert string like hh:mm:ss to a `datetime.timedelta`
    object. If `time_string` is None, return None.

    Parameters
    ----------
    time_string : string or None

    Returns
    -------
    td : None or datetime.timedelta

    """
    if time_string is None:
        return time_string
    fields = time_string.split(':')
    field_order = ['days', 'hours', 'minutes', 'seconds']
    kwargs = {}
    for field_num, field in enumerate(fields[::-1]):
        unit = field_order[-1 - field_num]
        kwargs[unit] = float(field)
    return timedelta(**kwargs)


def to_bool(bool_string):
    """Convert string to bool or keep None if None"""
    if bool_string is None:
        return None
    bool_string_lower = bool_string.lower()
    if bool_string_lower in ['1', 'true']:
        return True
    if bool_string_lower in ['0', 'false']:
        return False
    raise ValueError('Unrecognized value for bool: "%s"' % bool_string)


def to_int(int_string):
    """Convert string to int or keep None if None"""
    return int_string if int_string is None else int(int_string)


def sec_since_epoch_to_datetime(sec, tz=TZ_UTC):
    """Convert seconds since epoch into `datetime.datetime` object.

    Parameters
    ----------
    sec : None or convertible to float

    tz : datetime.tzinfo
        Timezone to convert to in output datetime.datetime object. Defaults to
        TZ_UTC (which is safest to use since it doesn't suffer from e.g.
        unexpected daylight savings shifting quirks in the Python datetime
        implementation...).

    Returns
    -------
    dt : None or datetime.datetime
        If `sec` is None, returns None; otherwise, converts to
        datetime.datetime

    """
    if sec is None:
        return sec
    if isinstance(sec, basestring):
        sec = float(sec)
    dt = datetime.fromtimestamp(sec, tz=TZ_UTC).astimezone(tz)
    return dt


NUM_RE = re.compile(r'(?P<mag>\d+)(?P<scale>[kmgtpe]){0,1}(?:[b]{0,1})',
                    re.IGNORECASE)

def to_bytes_size(qstat_size):
    """Convert a qstat size string to int bytes.

    Parameters
    ----------
    qstat_size : string
        E.g. '1' or '1b' -> 1 byte, '1kb' -> 1 KiB, '1mb' -> 1 MiB, etc.

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
    match = NUM_RE.match(qstat_size)
    if match is None:
        raise ValueError('Failed to parse quantity "%s"' % qstat_size)
    groupdict = match.groupdict()
    if groupdict['scale'] is not None:
        factor = scales[groupdict['scale'].lower()]
    else:
        factor = 1
    return int(ceil(float(groupdict['mag']) * factor))


GPUS_RE = re.compile(
    r'''
    gpus=(?P<gpus>\d+)             # gpus= technically 0-4 spec
    (
        (?P<shared>[:]shared)      # followed by :shared
        |                          # ... or ...
        (?P<reseterr>[:]reseterr)  # followed by :reseterr
    ){0,2}'                        # 0, 1, or 2 of these (can't handle dupes)
    ''', re.VERBOSE | re.IGNORECASE
)

QOS_RE = re.compile(r'qos=([^:]+)', re.IGNORECASE)
NODES_RE = re.compile(r'nodes=(\d+)', re.IGNORECASE)
PPN_RE = re.compile(r'ppn=(\d+)', re.IGNORECASE)
WALLTIME_RE = re.compile(r'walltime=((?:\d+:){0,3}(?:[0-9.]+))', re.IGNORECASE)
MEM_RE = re.compile(r'mem=([0-9.]+[kmgtep]?[b]?)', re.IGNORECASE)


def parse_pbs_command_file(path):
    r"""

    Job's "gpu=..." specifications imply:
    * Either not specified or gpus=0 -> no-GPU job (cyberlamp or ACI)
    * gpus=1 -> -A cyberlamp -l qos=cl_gpu
    * gpus={2, 3, 4} -> -A cyberlamp -l qos=cl_higpu

    If the job calls out walltime:
    *   0 < walltime <= 24 hours -> any cluster, any queue
    *  24 < walltime <= 96 hours -> NOT: ACI open
    *  96 < walltime <= 169 hours -> NOT: ACI open or CyberLAMP open
    * 169 < walltime        hours -> NOT: anywhere!

    If the job calls out Nodes & CPUs:

    If the job calls out memory:

    Returns
    -------
    info : dict
        `info` dict has the format:
            {
                'account': <int or None>,
                'qos': <string or None>
                'nodes': <int or None>,
                'ppn': <int or None>,
                'walltime': <datetime.timedelta None>
                'mem': <datetime.timedelta None>
                'gpus': <int>,
                'shared': <bool>,
                'reseterr': <bool>,
                'mail_users': <string or None>,
                'header': <list of \n-terminated strings>,
                'body': <list of \n-terminated strings>,
            }
        Note that `'header'` has any
            #PBS -A <account>
        and
            #PBS -l qos=<qos>
        lines removed so that we can implement these dynamically without
        the user's spec(s) interfering.

    """
    info = dict(
        path=expand(path),
        # -A account_string
        account=None,
        # -d path (TODO)
        workdir_path=None,
        # -D path (TODO)
        rootdir_path=None,
        # -e path
        stderr=None,
        # -f
        fault_tolerant=False,
        # -F "quoted string"
        script_args=None,
        # -j join {oe -> both to stdout, eo -> both to stderr}
        join=False,
        # -k keep {e, o, eo, oe, n}
        keep=False,
        # -l resource_list (non-NUMA specs implemented as of now!)
        nodes=None,
        ppn=None,
        walltime=None,
        mem=None,
        gpus=0,
        shared=False,
        reseterr=False,
        qos=None,
        # -m mail_options:
        # {
        #   n -> no emails for "normal" (non-error) conditions,
        #   p -> no emails whatsoever,
        #   a -> when aborted,
        #   b -> when begins execution,
        #   e -> when job terminates,
        #   f -> when job terminates w/ non-0 exit code
        # }
        mail_options=None,
        # -M user_list
        mail_users=None,
        # -n node-exclusive (illegal at ICS)
        node_exclusive=False,
        # -o path
        stdout=None,
        # -p priority (illegal at ICS)
        priority=None,
        # -P (probably illegal)
        as_user=None,
        # -q destination (illegal)
        destination=None,
        # -S path_list
        shell_path_list=None,
        # -u user_list
        user_list=None,
        # -v variable_list
        export_vars=None,
        # -V
        export_environment=False,
        # -W additional_attributes (some can be spec'd via other cmdline args)
        depend=None,
        group_list=None, # want: dfc13_collab
        interactive=False, # Also: -I
        job_radix=None,
        stagein=None,
        stageout=None,
        umask=None, # want: 0660
        # -x
        interactive_mode_execute_script=False,
        # -X
        x11_forwarding=False,
        # -z
        quiet_qsub=False,
        # PBS command file header
        header=[],
        # PBS command file body (i.e., the actual script)
        body=[]
    )

    in_header = True
    with open(info['path'], 'rU') as pbs_file:
        for line in pbs_file:
            if not in_header:
                info['body'].append(line)
                continue

            # Don't care about leading or trailing whitespace
            stripped = line.strip()

            # First non-blank / non-comment line means we're in the body
            if not (stripped == '' or stripped.startswith('#')):
                in_header = False
                info['body'].append(line)
                continue

            # Only care about PBS commands
            if not stripped.startswith('#PBS'):
                info['header'].append(line)
                continue

            command = stripped[4:].strip()

            # "command" has form -l <...>, -A <...> or -x, etc.
            flag = command[1]
            if len(command) > 2:
                arg = command[2:].strip()
            else:
                arg = ''

            if flag == 'A':
                info['account'] = arg

            elif flag == 'd':
                info['header'].append(line)
                info['workdir_path'] = arg

            elif flag == 'D':
                info['header'].append(line)
                info['rootdir_path'] = arg

            elif flag == 'e':
                info['header'].append(line)
                info['stderr'] = arg

            elif flag == 'f':
                info['header'].append(line)
                info['fault_tolerant'] = True

            elif flag == 'F':
                info['header'].append(line)
                info['script_args'] = arg

            elif flag == 'j':
                info['header'].append(line)
                info['join'] = True

            elif flag == 'k':
                info['header'].append(line)
                info['keep'] = True

            elif flag == 'l':
                info['header'].append(line)
                for match in NODES_RE.finditer(arg):
                    info['nodes'] = int(match.groups()[0])
                    break

                for match in PPN_RE.finditer(arg):
                    info['ppn'] = int(match.groups()[0])
                    break

                for match in WALLTIME_RE.finditer(arg):
                    info['walltime'] = (
                        ddhhmmss_to_timedelta(match.groups()[0])
                    )
                    break

                for match in MEM_RE.finditer(arg):
                    info['walltime'] = to_bytes_size(match.groups()[0])
                    break

                for match in GPUS_RE.finditer(arg):
                    groupdict = match.groupdict()
                    info['gpus'] = int(groupdict['gpus'])
                    info['shared'] = (
                        True if groupdict['shared'] else False
                    )
                    info['reseterr'] = (
                        True if groupdict['reseterr'] else False
                    )
                    break

                for match in QOS_RE.finditer(arg):
                    info['qos'] = match.groups()[0]
                    break

            elif flag == 'm':
                info['header'].append(line)
                if info['mail_options'] is None:
                    current = set([])
                else:
                    current = set(info['mail_options'])
                info['mail_options'] = set(arg).union(current)

            elif flag == 'M':
                info['header'].append(line)
                info['mail_users'] = arg

            elif flag == 'I':
                # Illegal; cannot do interactive jobs via openQ
                info['interactive'] = True

            elif flag == 'p':
                # Illegal; cannot set priority
                info['priority'] = arg

            elif flag == 'P':
                # Illegal; cannot set arbitrary user (not root)
                info['as_user'] = arg

            elif flag == 'q':
                # Illegal; cannot set arbitrary user (not root)
                info['destination'] = arg

            elif flag == 'S':
                info['header'].append(line)
                info['shell_path_list'] = arg

            elif flag == 'u':
                # Illegal; cannot set arbitrary user
                info['user_list'] = arg

            elif flag == 'v':
                # Illegal; openQ not set up to export vars
                info['export_vars'] = True

            elif flag == 'V':
                # Illegal; openQ not set up to export environment
                info['export_environment'] = True

            elif flag == 'W':
                # openQ constructs and provides additional_attributes,
                # so don't append this (these) line(s) to header
                attr_list = arg.split(',')
                for attr in attr_list:
                    vals = attr.split('=')
                    attr_name = vals[0].lower()
                    attr_val = '='.join(vals[1:])
                    # Simple strings...
                    if attr_name in ['depend', 'group_list', 'stagein',
                                     'stageout']:
                        info[attr_name] = attr_val
                    elif attr_name == 'interactive':
                        info['interactive'] = to_bool(attr_val)
                    elif attr_name == 'job_radix':
                        info['interactive'] = int(attr_val)

            elif flag == 'x':
                # Illegal; openQ does not do interactive mode
                info['interactive_mode_execute_script'] = True

            elif flag == 'X':
                # Illegal; openQ does not do interactive mode
                info['x11_forwarding'] = True

            elif flag == 'z':
                # Illegal; openQ forces non-quiet operation so we can
                # record messges to help users understand what's going
                # on
                info['quiet_qsub'] = True

            else:
                info['header'].append(line)

    return info


def gpu_access():
    """Does current user have access to CyberLAMP GPU queues?

    Returns
    -------
    gpu_access_by_cluster : dict
        Keys are cluster names and values are bools (True for access to GPUs
        on that cluster)

    """
    my_groups = [getgrgid(gid).gr_name for gid in getgroups()]
    gpu_access_by_cluster = {}
    for cluster, required_groups in GPU_GROUPS.items():
        gpu_access_by_cluster[cluster] = True
        for required_group in required_groups:
            if required_group not in my_groups:
                gpu_access_by_cluster[cluster] = False
                break
    return gpu_access_by_cluster
