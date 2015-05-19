"""
Command-line interface for the Dataset Client
"""
import sys

import os
import socket
import time




###  TODO: remove dataset_client.py from this directory
#from scidataspace.client.globusonline.catalog.client.dataset_client import dataset_client






from models import File, Outbox, create_default_name_path_rule
from dao import OutboxStateDAO
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

__DEFAULT_OUTBOX_NAME = "outbox"
__BULK_OPS_MAX = 1000


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

def cmd_main(root , catalog_id, dataset_id, dataset_client):

    # Use home directory as default location for state.db
    default_state_db = os.path.join(os.path.expanduser('~'),
                                    '.tagfiler', 'state.db')

    # Use username#hostname as default endpoint
    default_endpoint = "gsiftp://%s" % socket.gethostname()

    '''
    # create the parser for the "create_member" command
    parser_create_member = subparsers.add_parser('add_members', help='create_members help')
#    parser_create_member.add_argument('--data_type', type=str, choices=['file', 'directory'], help='data type')
#    parser_create_member.add_argument('--data_uri', type=str, help='data uri')
    parser_create_member.set_defaults(operation="create_members")
    '''

    # Create outbox model, and populate from settings
    outbox_model = Outbox()
    outbox_model.name = __DEFAULT_OUTBOX_NAME
    outbox_model.state_db = default_state_db

    # Tagfiler settings
    outbox_model.url = dataset_client.DEFAULT_BASE_URL
    outbox_model.globus_token = dataset_client.globus_token

    outbox_model.dataset_id = dataset_id
    #outbox_model.catalog_name = args.catalog_name
    outbox_model.catalog_id = catalog_id
    outbox_model.bulk_ops_max = __BULK_OPS_MAX
    outbox_model.bulk_ops_max = int(outbox_model.bulk_ops_max)

    # Endpoint setting
    outbox_model.endpoint = default_endpoint

    # starting notifier to make sure that all files are processed when this program will end
    pyinotifier_watcher = pyinotify.WatchManager()  # Watch Manager


    notifier = pyinotify.ThreadedNotifier(pyinotifier_watcher, NotifierEventHandler())
    notifier.start()

    # Roots
    outbox_model.roots.append(root)
    pyinotifier_watched_directories = pyinotifier_watcher.add_watch(root, pyinotifier_mask, rec=True)

    # Add the default 'name' tag path rule
    name_rule = create_default_name_path_rule(outbox_model.endpoint)
    outbox_model.path_rules.append(name_rule)

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

    # Tag files in worklist
    #tag_director = TagDirector()
    for f in worklist:
        logger.debug("Tagging: %s" % f)
        print f
        data_type = "directory" if f.size is None else "file"
        try:
            dataset_client.create_member(catalog_id, dataset_id, dict(data_type=data_type, data_uri=f.filename))
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

    print "Done."



'''
cmd_to_run = "python "+current_path+"/../members/cmdline.py --root  "+cmd_2+ \
             " --dataset_id "+str(geounit_id)+" --catalog_name "+catalog_name+\
             " --catalog_id "+str(catalog_id)+" add_members"


'''

# if __name__ == "__main__":
#     pass
#     # For testing with ipython
#     #cmd_main(sys.argv[1:])
#
#     #sciunit.py

