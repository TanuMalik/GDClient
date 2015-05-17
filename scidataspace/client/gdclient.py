__author__ = 'cristian'

import os, sys, subprocess
import datetime,json, re
from pprint import pprint

from globusonline.catalog.client.goauth import get_access_token

import code
import logging
import readline
import requests
import atexit

import configparser

from leveldb import LevelDB, LevelDBError
from globusonline.catalog.client.dataset_client import DatasetClient
from scidataspace.client.completer import BufferAwareCompleter


global datasetClient

def init_DatasetClient(goauth_token,BASE_URL):
    datasetClient = DatasetClient(goauth_token, BASE_URL)
    if datasetClient is None:
        print "cannot obtain a valid dataset client"
        exit(1)


def cmd_print_output(process):
    for line in iter(process.stdout.readline, ''):
        line = line.strip()
        print "   > ", line
    return 1


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

def get_cfg_field(field):
    try:
        return str(config.get('Default', field))
    except:
        print "%s is not defined"% field
        return "None"

## Read the config.ini file and check if URL is set
## if not ask to set it and exit
## if username is none ask for username and store it config.ini. 
## Next time the client is run, read username from config.ini
## If Globus token is none, Obtain Globus token and store it, else proceed 
## Return config
def gd_init(config_file_name):

    config.read_file(open(config_file_name))
    if get_cfg_field('URL') == "None":
        print "GeoDataspace URL is not set"
        exit(1)

    b_will_exit = False
    if  get_cfg_field('uname') == "None":
        value = raw_input("Please provide user name > ")

    if get_cfg_field('goauth-token') == "None":
        b_will_exit = True
        try:
            result = get_access_token(username=config['Default']['uname'])
            config['Default']['uname']=value
            config['Default']['goauth-token']= result.token
	    b_will_exit = False
        except Exception as e:
	    print "There was an error in obtaining Globus token. Please check username or your password"
            sys.stderr.write(str(e) + "\n")

    if b_will_exit:
        with open(config_file_name, 'w') as configfile:
            config.write(configfile)
        #print "Thank you for setup. Please run client again"
        #exit(1)
    return config

if __name__ == '__main__':
   
    config_file_name = ".gdclient/config.ini"
    config = configparser.ConfigParser()
    config = gd_init(config_file_name)
    
    ## Init a datasetclient
    datasetClient = DatasetClient(config['Default']['URL'], config['Default']['goauth-token'])
    ## If datasetclient is not None then connection between client and server established. 
    if datasetClient is None:
        print "cannot obtain a valid dataset client"
        exit(1)

    ## check if the LevelDB local database and histfile exists; if not create; if yes re-use	
    ## LevelDB local database
    levelDB_local_database = ".gdclient/.gdclient_levelDB"
    ## Add history
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

    '''
    geounit_name = UNDEFINED
    geounit_id = "0"
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
    '''


