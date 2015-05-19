"""
Command-line interface for the Dataset Client
"""
import sys
import argparse

import os
import socket
import time
import re
import json

###  TODO: remove dataset_client.py from this directory
import dataset_client
#from scidataspace.client.globusonline.catalog.client.dataset_client import dataset_client

from models import File, Outbox, create_default_name_path_rule
from dao import OutboxStateDAO
from tagfilerutil.rules import TagDirector
from tagfilerutil.files import tree_scan_stats, create_uri_friendly_file_path, sha256sum

import logging
logger = logging.getLogger(__name__)

import pyinotify
pyinotifier_mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE | pyinotify.IN_DELETE_SELF | \
    pyinotify.IN_DONT_FOLLOW | pyinotify.IN_MOVE_SELF | pyinotify.IN_MOVED_FROM | \
    pyinotify.IN_MOVED_TO | pyinotify.IN_UNMOUNT

# Exit return codes
__EXIT_SUCCESS = 0
__EXIT_FAILURE = 1

# Used by ArgumentParser
__PROG = "dataset-client"
__DESC = "The Dataset Client command-line utility."
#__VER  = "%(prog)s " + ("%d.%d trunk" % (version.MAJOR, version.MINOR))
__DEFAULT_OUTBOX_NAME = "outbox"
__BULK_OPS_MAX = 1000

# Verbosity to Loglevel dictionary
__LOGLEVEL = {0: logging.ERROR,
              1: logging.WARNING,
              2: logging.INFO,
              3: logging.DEBUG}
__LOGLEVEL_MAX = 3
__LOGLEVEL_DEFAULT = 0


class NotifierEventHandler(pyinotify.ProcessEvent):
    def process_IN_CREATE(self, event):
        print "Creating:", event.pathname

    def process_IN_DELETE(self, event):
        print "Removing:", event.pathname

    def process_IN_DELETE_SELF(self, event):
        print "Removing self:", event.pathname

    def process_IN_DONT_FOLLOW(self, event):
        print "Not following symlink:", event.pathname

    def process_IN_MOVE_SELF(self, event):
        print "Moving self:", event.pathname

    def process_IN_MOVED_FROM(self, event):
        print "Moving from:", event.pathname

    def process_IN_MOVED_TO(self, event):
        print "Moving to:", event.pathname

    def process_IN_UNMOUNT(self, event):
        print "Unmounted:", event.pathname

def main(args=None):
    """
    The main routine.
    
    Optionally accepts 'args' but this is more of a convenience for unit 
    testing this module. It passes 'args' directly to the ArgumentParser's
    parse_args(...) method.
    """
    parser = argparse.ArgumentParser(prog=__PROG, description=__DESC)

    # General options
#    parser.add_argument('--version', action='version', version=__VER)

    # Directory and Inclusion/Exclusion option group
#    group = parser.add_argument_group(title='Directory traversal options')
#    group.add_argument('--catalog_id', metavar='CATALOGID',
#                       type=int, nargs='+',
#                       help='catalog id to access')

    # Outbox name
    helpstr=('name of the outbox configuration (default: %s)' %
             __DEFAULT_OUTBOX_NAME)
    parser.add_argument('-n', '--name', type=str, help=helpstr)

    # Use home directory as default location for outbox.conf
    default_config_filename = os.path.join(
            os.path.expanduser('~'), '.tagfiler', 'outbox.conf')
    parser.add_argument('-f', '--filename', type=str,
                        help=('configuration filename (default: %s)' %
                              default_config_filename))

    # Use home directory as default location for state.db
    default_state_db = os.path.join(os.path.expanduser('~'),
                                    '.tagfiler', 'state.db')
    parser.add_argument('-s', '--state_db', type=str,
                        help=('local state database (default: %s)' %
                              default_state_db))

    # Use username#hostname as default endpoint
    default_endpoint = "gsiftp://%s" % socket.gethostname()
    parser.add_argument('-e', '--endpoint', type=str,
                        help=('endpoint (default: %s)' %
                              default_endpoint))

    # Verbose | Quite option group
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', '--verbose', action='count', 
                       default=__LOGLEVEL_DEFAULT, 
                       help='verbose output (repeat to increase verbosity)')
    group.add_argument('-q', '--quiet', action='store_true', 
                       help='suppress output')

    # Directory and Inclusion/Exclusion option group
    group = parser.add_argument_group(title='Directory traversal options')
    group.add_argument('--root', metavar='DIRECTORY',
                       type=str, nargs='+',
                       help='root directories to be traversed recursively')
    group.add_argument('--exclude', type=str, nargs='+',
                       help='exclude based on regular expression')
    group.add_argument('--include', type=str, nargs='+',
                       help='include based on regular expression')

    # Tagfiler option group
    group = parser.add_argument_group(title='Tagfiler options')
    group.add_argument('--url', dest='url', metavar='URL', 
                       type=str, help='URL of the Tagfiler service')
    group.add_argument('--username', dest='username', metavar='USERNAME', 
                       type=str, help='username for your Tagfiler user account')
    group.add_argument('--password', dest='password', metavar='PASSWORD', 
                       type=str, help='password for your Tagfiler user account')
    group.add_argument('--globus_token', dest='globus_token', metavar='GLOBUS_TOKEN', 
                       type=str, help='Globus token for your Tagfiler user account')
    group.add_argument('--bulk_ops_max', type=int,
                        help='maximum bulk operations per call to Tagfiler' + \
                        ' (default: %d)' % __BULK_OPS_MAX)
    group.add_argument('--dataset_id', dest='dataset_id', metavar='DATASET_ID',
                       type=str, help='Dataset ID to add files to')
    group.add_argument('--catalog_name', dest='catalog_name', metavar='CATALOG_NAME', 
                       type=str, help='Catalog to add files to', required=False)
    group.add_argument('--catalog_id', dest='catalog_id', metavar='CATALOG_ID',
                       type=str, help='Catalog ID to add files to', required=False)
#    group.add_argument('--path', dest='walk_path',3
#                       type=str, help='Path to get files from', required=True)

    subparsers = parser.add_subparsers(help='sub-command help')

    # create the parser for the "create_member" command
    parser_create_member = subparsers.add_parser('add_members', help='create_members help')
#    parser_create_member.add_argument('--data_type', type=str, choices=['file', 'directory'], help='data type')
#    parser_create_member.add_argument('--data_uri', type=str, help='data uri')
    parser_create_member.set_defaults(operation="create_members")

    # create the parser for the "create_dataset" command
    parser_create_dataset = subparsers.add_parser('create_dataset', help='create_dataset help')
    parser_create_dataset.add_argument('--name', type=str, help='dataset name')
    parser_create_dataset.set_defaults(operation="create_dataset")

    # Now parse them
    args = parser.parse_args(args)

    # Turn verbosity into a loglevel setting for the global logger
    if args.quiet:
        logging.getLogger().addHandler(logging.NullHandler())
        # Should probably suppress stderr and stdout
    else:
        verbosity = args.verbose if args.verbose < __LOGLEVEL_MAX else __LOGLEVEL_MAX
        logging.basicConfig(level=__LOGLEVEL[verbosity])
        logger.debug("args: %s" % args)

    # Load configuration file, or create configuration based on arguments
    filename = args.filename or default_config_filename
    cfg = {}
    if os.path.exists(filename):
        f = open(filename, 'r')
        try:
            cfg = json.load(f)
            logger.debug("config: %s" % cfg)
        except ValueError as e:
            print >> sys.stderr, ('ERROR: Malformed configuration file: %s' % e)
            return __EXIT_FAILURE
        else:
            f.close()

    # Create outbox model, and populate from settings
    outbox_model = Outbox()
    outbox_model.name = args.name or cfg.get('name', __DEFAULT_OUTBOX_NAME)
    outbox_model.state_db = args.state_db or \
                            cfg.get('state_db', default_state_db)

    # Tagfiler settings
    outbox_model.url = args.url or dataset_client.DEFAULT_BASE_URL
    if not outbox_model.url:
        parser.error('Tagfiler URL must be given.')
    """
    outbox_model.username = args.username or cfg.get('username')
    if not outbox_model.username:
        parser.error('Tagfiler username must be given.')

    outbox_model.password = args.password or cfg.get('password')
    if not outbox_model.password:
        parser.error('Tagfiler password must be given.')

    outbox_model.globus_token = args.globus_token or cfg.get('globus_token')
    """
    outbox_model.dataset_id = args.dataset_id or cfg.get('dataset_id')
    outbox_model.catalog_name = args.catalog_name or cfg.get('catalog_name')
    outbox_model.bulk_ops_max = args.bulk_ops_max or \
                                cfg.get('bulk_ops_max', __BULK_OPS_MAX)
    outbox_model.bulk_ops_max = int(outbox_model.bulk_ops_max)

    # Endpoint setting
    outbox_model.endpoint = args.endpoint or \
                                cfg.get('endpoint', default_endpoint)


    # starting notifier to make sure that all files are processed when this program will end
    pyinotifier_watcher = pyinotify.WatchManager()  # Watch Manager


    notifier = pyinotify.ThreadedNotifier(pyinotifier_watcher, NotifierEventHandler())
    notifier.start()


    # Roots
    roots = args.root or cfg.get('roots')
    if not roots or not len(roots):
        parser.error('At least one root directory must be given.')
    for root in roots:
        outbox_model.roots.append(root)
        pyinotifier_watched_directories = pyinotifier_watcher.add_watch(root, pyinotifier_mask, rec=True)

    # Add include/exclusion patterns
    excludes = args.exclude or cfg.get('excludes')
    if excludes and len(excludes):
        for exclude in excludes:
            pathlist = re.compile(exclude)
            for path in pathlist:
                outbox_model.excludes.append(path)
                pyinotifier_watched_directories = pyinotifier_watcher.rm_watch(path, pyinotifier_mask, rec=True)

    includes = args.include or cfg.get('includes')
    if includes and len(includes):
        for include in includes:
            pathlist = re.compile(include)
            for path in pathlist:
                outbox_model.includes.append(path)
                pyinotifier_watched_directories = pyinotifier_watcher.add_watch(path, pyinotifier_mask, rec=True)


    # Add the default 'name' tag path rule
    name_rule = create_default_name_path_rule(outbox_model.endpoint)
    outbox_model.path_rules.append(name_rule)

# --catalog_name aaa1 create_dataset --name name1
# --dataset_id 152 --catalog_name aaa1 create_member --data_type file --data_uri file://some_demo_file2


    # Establish Tagfiler client connection
    # will be here

    state = OutboxStateDAO(outbox_model.state_db)
    worklist = []
    found = 0
    skipped = 0
    tagged = 0
    registered = 0

    # walk the root trees, cksum as needed, create worklist to be registered
    for root in outbox_model.roots:
        for (rfpath, size, mtime, user, group) in \
                tree_scan_stats(root, outbox_model.excludes, outbox_model.includes):
            filename = create_uri_friendly_file_path(root, rfpath)
            fargs = {'filename': filename, 'mtime': mtime, 'size': size, \
                    'username': user, 'groupname': group}
            f = File(**fargs)
            found += 1

            # Check if file exists in local state db
            exists = state.find_file(filename)
            if not exists:
                # Case: New file, not seen before
                logger.debug("New: %s" % filename)
                f.checksum = sha256sum(filename)
                state.add_file(f)
                worklist.append(f)
            elif f.mtime > exists.mtime:
                # Case: File has changed since last seen
                logger.debug("Modified: %s" % filename)
                f.checksum = sha256sum(filename)
                if f.checksum != exists.checksum:
                    f.id = exists.id
                    state.update_file(f)
                    worklist.append(f)
                else:
                    exists.mtime = f.mtime # update mod time
                    state.update_file(f)
                    skipped += 1
            elif f.size and not exists.checksum:
                # Case: Missing checksum, on regular file
                logger.debug("Missing checksum: %s" % filename)
                f.checksum = sha256sum(filename)
                f.id = exists.id
                state.update_file(f)
                worklist.append(f)
            elif not exists.rtime:
                # Case: File has not been registered
                logger.debug("Not registered: %s" % filename)
                worklist.append(exists)
            else:
                # Case: File does not meet any criteria for processing
                logger.debug("Skipping: %s" % filename)
                skipped += 1

    goauth_token = "un=tanum|tokenid=728fb046-49be-11e4-89f7-22000ab68755|expiry=1443740304|client_id=tanum|token_type=Bearer|SigningSubject=https://nexus.api.globusonline.org/goauth/keys/24e59702-45a4-11e4-89f7-22000ab68755|sig=49ccb4f2aff39f7f5d93272f403d5263235e9eb708e316a0445ba9857aba98d47e7f847871d936a93a715324ce1e1cb0fa992de698b194f6e14f3c1b8f4bee5731435eeab673c637f18fe526f00d630fb38c98a7e4ea58de1416caae5b6d79adc8d7a141c9e9e437098077212de3323cd4aef39d412119592756470bbfd45978"#sys.argv[1]
    base_url = dataset_client.DEFAULT_BASE_URL
    client = dataset_client.DatasetClient(goauth_token, base_url)

    #catalog=client.get_catalog_by_name(args.catalog_name)
    catalog_id = args.catalog_id
    dataset_id=61

    # Tag files in worklist
    tag_director = TagDirector()
    for f in worklist:
        logger.debug("Tagging: %s" % f)
        print f
        data_type = "directory" if f.size is None else "file"
        try:
            client.create_member(catalog_id, dataset_id, dict(data_type=data_type, data_uri=f.filename))
        except:
            print "Unexpected error:", sys.exc_info()[0]
            pass

        #client.add_member((worklist)
        #tag_director.tag_registered_file(outbox_model.path_rules, f)
        #tag_director.tag_registered_file(outbox_model.dicom_rules, f)
        #tag_director.tag_registered_file(outbox_model.nifti_rules, f)
        #tag_director.tag_registered_file(outbox_model.vcf_rules, f)
        #tag_director.tag_file_contents(outbox_model.line_rules, f)
        tagged += 1


    # Register files in worklist
    # client.add_subjects(worklist)
    for f in worklist:
        logger.debug("Registered: %s" % f)
        f.rtime = time.time()
        state.update_file(f)
        registered += 1

    # stopping pyinotifier; we cannot use events if client will be closed
    pyinotifier_watcher.rm_watch(pyinotifier_watched_directories.values())
    notifier.stop()

    # Print final message unless '--quiet'
    if not args.quiet:
        # Print concluding message to stdout
        print "Done."
    try:
        client.close()
    except:
        print >> sys.stderr, ('WARN: %s' % sys.exc_info()[0])

    return __EXIT_SUCCESS

if __name__ == "__main__":
    # For testing with ipython
    main(sys.argv[1:])

    #sciunit.py

