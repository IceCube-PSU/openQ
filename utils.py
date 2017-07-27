"""
Assortment of utility functions and classes
"""


from __future__ import absolute_import

from datetime import datetime, timedelta, tzinfo
from grp import getgrnam
from math import ceil
from os import chmod, chown, listdir, makedirs, remove, rmdir, stat, utime
from os.path import abspath, expanduser, expandvars, isdir, join
import re
from shutil import copy2, copytree
from sys import stderr, stdout
from time import altzone, daylight, localtime, mktime, time, timezone, tzname


__all__ = ['expand', 'set_path_metadata', 'wstdout', 'wstderr',
           'get_xml_subnode', 'get_xml_val', 'hhmmss_to_timedelta', 'to_bool',
           'to_int', 'sec_since_epoch_to_datetime', 'to_bytes_size',
           'UTCTimezone', 'TZ_UTC', 'LocalTimezone', 'TZ_LOCAL']


def expand(path):
    """Shortcut to expand path or string"""
    return abspath(expanduser(expandvars(path)))


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

    if isinstance(group, basestring):
        gid = getgrnam(group).gr_gid
    elif isinstance(group, int):
        gid = group
    else:
        assert group is None

    # Set permissions
    if perms is not None:
        chmod(path, perms)

    # Change group owner (note that -1 keeps user the same)
    if group is not None and stat(path).st_gid != gid:
        chown(path, -1, gid)

    # Change modification time on the file to `mtime`; set access time to now
    if mtime is not None:
        access_time = time()
        utime(path, (access_time, mtime))


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
            if err[0] != 39: # ignore dir not empty
                raise


def wstdout(msg):
    """Write `msg` to stdout & flush immediately"""
    stdout.write(msg)
    stdout.flush()


def wstderr(msg):
    """Write `msg` to stderr & flush immediately"""
    stderr.write(msg)
    stderr.flush()


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


def hhmmss_to_timedelta(time_string):
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
    return bool_string if bool_string is None else bool(bool_string)


def to_int(int_string):
    """Convert string to int or keep None if None"""
    return int_string if int_string is None else int(int_string)


def sec_since_epoch_to_datetime(sec):
    """Convert seconds since epoch into `datetime.datetime` object

    Parameters
    ----------
    sec : None or convertible to float

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
    return datetime.fromtimestamp(sec, tz=TZ_UTC).astimezone(TZ_LOCAL)


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
