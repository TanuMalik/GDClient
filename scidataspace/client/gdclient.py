__author__ = 'cristian'

import os, sys, subprocess
import datetime,json, re
from pprint import pprint

import code
import logging
import readline
import requests
import atexit

from leveldb import LevelDB, LevelDBError
from globusonline.catalog.client.dataset_client import DatasetClient
from scidataspace.client.completer import BufferAwareCompleter



def run_command(args, processing_function=cmd_print_output):
    #print ("running:",args)
    try:
        process = subprocess.Popen(args,
                                   shell=True,
                                   bufsize=-1,
                                   stdout=subprocess.PIPE,
                                   stderr=None,
                                   universal_newlines=True)


        output = processing_function(process)

        process.communicate()
    except OSError:
        print "   > ", sys.exc_info()[1]

    return output

if __name__ == '__main__':

    # check if the LevelDB local database and histfile exists; if not create; if yes re-use	
    ### LevelDB local database
    levelDB_local_database = ".gdclient/.gdclient_levelDB"
    ### Add history
    histfile = ".gdclient/.gd_history"
    try:
        readline.read_history_file(histfile)
    except IOError:
        pass
    atexit.register(readline.write_history_file, histfile)
    del histfile

    db = LevelDB(levelDB_local_database)
    if db == None:
        print("cannot open db from "+levelDB_local_database)
        exit(1)

    print ('enter "stop" to end session')
    completer_suggestions = {
         'geounit':['start', 'delete'],
         'add_member':[],
         'annotate':['geounit', 'member'],
         'stop':[]
    }
    readline.set_completer(BufferAwareCompleter(completer_suggestions).complete)

    # Use the tab key for completion
    readline.parse_and_bind('tab: complete')


    while True :
        raw_cmd = raw_input(geounit_name+" > ")
        cmd_to_run = raw_cmd
        cmd_splitted = SafeList(raw_cmd.split())

        if cmd_splitted.get(0) == "stop":
            break
        elif cmd_splitted.get(0)=="geounit":
            parse_cmd_geounit(cmd_splitted)

        elif cmd_splitted.get(0)=="annotate":
            parse_cmd_annotate(cmd_splitted)

        elif cmd_splitted.get(0)=="add_member":
            parse_cmd_add_member(cmd_splitted)

        else:

            # any bash command that we want to pass to the system
            #print "any cmd"
            run_command(cmd_to_run)
    print "done"


