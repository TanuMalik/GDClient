"""
File utilities for the tagfiler project.
"""

import os
import hashlib
import logging

logger = logging.getLogger(__name__)

def tree_scan(top, 
              expand_dir=lambda dirpath, relpath: relpath, 
              expand_file=lambda dirpath, relpath, fname: '%s%s%s' % (relpath, os.path.sep, fname), 
              excludes=[],
              includes=[]):
    """Generate a sequence of file-tree members with optional filtering and transformation.

       Required 'top' parameter is string path to root directory; a
       non-directory entry will generate zero members.

       Optional 'expand_dir' function takes (dirpath, relpath) of a
       member directory and returns alternate value (default returns
       relpath).

       Optional 'expand_file' function takes (dirpath, relpath, fname)
       of a member file and returns alternate value (default returns
       relative-qualified file name).

       Optional 'excludes' is a list of pattern objects with a
       search(string) method returning truth values on excluded names.

       Optional 'includes' is a list of pattern objects with a
       search(string) method returning truth values on included names.

       Pattern searches use the basename and a qualified name rooted
       at the level of the top parameter. So while searching
       top='/usr' and encountering a member '/usr/lib/X11', the actual
       search test for a pattern p would be:

          p.search('X11') or p.search('/lib/X11')

       Thus, a pattern can satsify an arbitrary unanchored substring,
       or a string anchored on basename, or a substring containing '/'
       hierarchy, or a string anchored with '/' at the root of the
       scanned tree.  Useful patterns can be obtained from the
       re.compile() constructor:

          excludes=[ re.compile('^\.svn'), re.compile('^/tmp'), re.compile('~$') ]

       Filtering logic is to exclude members matching at least one
       exclusion pattern unless that member also matches at least one
       inclusion pattern. Default for no exclusion pattern matches is
       to include the member. When a directory member is excluded, its
       descendents are also excluded implicitly.
       
    """
    
    def exclude_name(name, relpath):
        drop = False
        for p in excludes:
            if p.search(name) or p.search('%s%s%s' % (relpath, os.path.sep, name)):
                drop = True
        for p in includes:
            if p.search(name) or p.search('%s%s%s' % (relpath, os.path.sep, name)):
                drop = False
        return drop

    def filter_list_inplace(l, dirpath=None):
        for i in [ len(l) - 1 - x for x in range(0, len(l)) ]:
            if exclude_name(l[i], dirpath):
                del l[i]

    for dirpath, dirnames, filenames in os.walk(top):
        relpath = dirpath[len(top):]
            
        yield expand_dir(dirpath, relpath or os.path.sep)

        # filter out excluded children
        filter_list_inplace(dirnames, relpath)
        filter_list_inplace(filenames, relpath)

        for fname in filenames:
            # non-excluded child files are not visited again
            yield expand_file(dirpath, relpath, fname)

def uid2uname(uid):
    """Convert numerid UID to username if possible or leave as number."""
    try:
        import pwd
        return pwd.getpwuid(uid)[0]
    except ImportError:
        logger.warn("Python module pwd cannot be found.")
    except:
        pass
    return uid

def gid2gname(gid):
    """Convert numeric GID to groupname if possible or leave as number."""
    try:
        import grp
        return grp.getgrgid(gid)[0]
    except ImportError:
        logger.warn("Python module grp not found.")
    except:
        pass
    
    return gid

def expand_dir_stats(dirpath, relpath):
    """Expand directory stats as a helper function useful with tree_scan expand_dir argument.

       Returns tuple (dirpath, None, mtime, user, group)
    """
    try:
        s = os.stat(dirpath)
    except:
        s = os.lstat(dirpath)
    return (relpath, None, s.st_mtime, uid2uname(s.st_uid), gid2gname(s.st_gid))

def expand_file_stats(dirpath, relpath, fname):
    """Expand file stats as a helper function useful with tree_scan expand_file argument.

       Returns tuple (filepath, size, mtime, user, group)
       size may be None if fname does not link to a regular file
    """
    fpath = '%s%s%s' % (dirpath, os.path.sep, fname)
    rfpath = '%s%s%s' % (relpath, os.path.sep, fname)
    try:
        s = os.stat(fpath)
        return (rfpath, s.st_size, s.st_mtime, uid2uname(s.st_uid), gid2gname(s.st_gid))
    except:
        s = os.lstat(fpath)
        return (rfpath, None, s.st_mtime, uid2uname(s.st_uid), gid2gname(s.st_gid))

def sha256sum(fpath):
    """Return hex digest string like sha256sum utility would compute."""
    h = hashlib.sha256()
    try:
        f = open(fpath, 'rb')
        try:
            b = f.read(4096)
            while b:
                h.update(b)
                b = f.read(4096)
            return h.hexdigest()
        finally:
            f.close()
    except:
        return None

def expand_dir_stats_sha256(dirpath, relpath):
    """Expand directory stats as a helper function useful with tree_scan expand_dir argument.

       Returns tuple (dirpath, None, mtime, user, group, None)
    """
    try:
        s = os.stat(dirpath)
    except:
        s = os.lstat(dirpath)
    return (relpath, None, s.st_mtime, uid2uname(s.st_uid), gid2gname(s.st_gid), None)

def expand_file_stats_sha256(dirpath, relpath, fname):
    """Expand file stats as a helper function useful with tree_scan expand_file argument.

       Returns tuple (filepath, size, mtime, user, group, sha256sum)
       size may be None if fname does not link to a regular file
       sha256sum may be None if fname does not link to a readable regular file
    """
    fpath = '%s%s%s' % (dirpath, os.path.sep, fname)
    rfpath = '%s%s%s' % (relpath, os.path.sep, fname)
    try:
        s = os.stat(fpath)
        return (rfpath, s.st_size, s.st_mtime, uid2uname(s.st_uid), gid2gname(s.st_gid), sha256sum(fpath))
    except:
        s = os.lstat(fpath)
        return (rfpath, None, s.st_mtime, uid2uname(s.st_uid), gid2gname(s.st_gid))

def tree_scan_stats(top, excludes=[], includes=[]):
    """Convenience wrapper for tree_scan."""
    return tree_scan(top, expand_dir=expand_dir_stats, expand_file=expand_file_stats, excludes=excludes, includes=includes)

def tree_scan_stats_sha256(top, excludes=[], includes=[]):
    """Convenience wrapper for tree_scan."""
    return tree_scan(top, expand_dir=expand_dir_stats_sha256, expand_file=expand_file_stats_sha256, excludes=excludes, includes=includes)

def create_uri_friendly_file_path(dir_path, rfilename):
    """
    Creates a full file path with uri-friendly path separators so that it can
    be used in a file:// uri
    """
    file_path = "%s%s" % (dir_path.replace("\\","/"), rfilename.replace("\\","/"))
    if file_path[0] != "/":
        file_path = "/%s" % file_path
    return file_path
