"""
Assortment of utility functions and classes
"""


from __future__ import absolute_import

from datetime import datetime, timedelta, tzinfo
from grp import getgrnam
from math import ceil
from os import chmod, chown, listdir, makedirs, remove, rmdir, stat
from os.path import abspath, expanduser, expandvars, isdir, join
import re
from shutil import copy2, copytree
from sys import stderr, stdout
from time import timezone, altzone, daylight, tzname, mktime, localtime


__all__ = ['expand', 'wstdout', 'wstderr', 'get_xml_subnode', 'get_xml_val',
           'hhmmss_to_timedelta', 'to_bool', 'to_int',
           'sec_since_epoch_to_datetime', 'to_bytes_size', 'UTCTimezone',
           'TZ_UTC', 'LocalTimezone', 'TZ_LOCAL']


def expand(s):
    """Shortcut to expand path or string"""
    return abspath(expanduser(expandvars(s)))


def mkdir(path, perms=0o770, group=None):
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
    if isinstance(group, basestring):
        gid = getgrnam(group).gr_gid
    elif isinstance(group, int):
        gid = group
    else:
        assert group is None

    # Make dir if doesn't exist; will error out if path exists but is a file
    # (as desired)
    if not isdir(path):
        makedirs(path)

    # Set permissions
    if perms is not None:
        chmod(path, perms)

    # Change group owner (note that -1 keeps user the same)
    if group is not None and stat(path).st_gid != gid:
        chown(path, -1, gid)


def copy_contents(srcdir, destdir):
    """Copy the contents of `srcdir` into `destdir`. Ignore errors due to .nfs*
    files refusing to be modified."""
    assert isdir(srcdir)
    assert isdir(destdir)
    for name in listdir(srcdir):
        source_path = join(srcdir, name)
        if not isdir(source_path):
            try:
                copy2(source_path, destdir)
            except IOError, err:
                if err[0] != 26: # and '.nfs' in err[1]):
                    raise
        else:
            dest_path = join(destdir, name)
            copytree(source_path, dest_path)


def remove_contents(target):
    """Remove the contents of `target` directory. Ignore errors due to .nfs*
    files refusing to be removed."""
    assert isdir(target)
    for name in listdir(target):
        source_path = join(target, name)
        if not isdir(source_path):
            try:
                remove(source_path)
            except IOError, err:
                if not (err[0] == 26 and '.nfs' in err[1]):
                    raise
        elif isdir(source_path):
            remove_contents(source_path)
            try:
                rmdir(source_path)
            except OSError, err:
                if err[0] != 39: # dir not empty is only OK
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
    fields = t.split(':')
    field_order = ['days', 'hours', 'minutes', 'seconds']
    kwargs = {}
    for field_num, field in enumerate(fields[::-1]):
        unit = field_order[-1 - field_num]
        kwargs[unit] = float(field)
    return timedelta(**kwargs)


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


NUM_RE = re.compile(r'(?P<mag>\d+)(?P<scale>[kmgtpe]){0,1}(?:[b]{0,1})',
                    re.IGNORECASE)

def to_bytes_size(s):
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
    match = NUM_RE.match(s)
    if match is None:
        raise ValueError('Failed to parse quantity "%s"' % s)
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
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, 0)
        stamp = mktime(tt)
        tt = localtime(stamp)
        return tt.tm_isdst > 0

TZ_LOCAL = LocalTimezone()
