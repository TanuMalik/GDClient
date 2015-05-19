__author__ = 'cristian'

import os, sys, subprocess
import datetime,json, re
from pprint import pprint


import code
import logging
import readline
import requests
import atexit

import configparser

from leveldb import LevelDB, LevelDBError
from globusonline.catalog.client.dataset_client import DatasetClient
from globusonline.catalog.client.goauth import get_access_token
from completer import BufferAwareCompleter
from query_dataset_client import get_catalogs, get_catalog_by_name, get_last_datasets

from commands.util import UNDEFINED, SafeList, run_command
from commands.annotate import parse_cmd_annotate
from commands.geounit import parse_cmd_geounit
from commands.members import parse_cmd_add_member

global datasetClient

def init_DatasetClient(goauth_token,BASE_URL):
    datasetClient = DatasetClient(goauth_token, BASE_URL)
    if datasetClient is None:
        print "cannot obtain a valid dataset client"
        exit(1)

class GDConfig:
    config_file_name = ".gdclient/config.ini"
    config = None

    ## Read the config.ini file and check if URL is set
    ## if not ask to set it and exit
    ## if username is none ask for username and store it config.ini.
    ## Next time the client is run, read username from config.ini
    ## If Globus token is none, Obtain Globus token and store it, else proceed
    ## Return config
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read_file(open(self.config_file_name))
        if self.get_cfg_field('URL') == "None":
            print "GeoDataspace URL is not set"
            exit(1)

        b_will_exit = False
        if  self.get_cfg_field('uname') == "None":
            uname = raw_input("Please provide user name > ")

        if self.get_cfg_field('goauth-token') == "None":
            b_will_exit = True
            try:
                result = get_access_token(username=uname)
                self.config['Default']['uname']=uname
                self.config['Default']['goauth-token']= result.token
                b_will_exit = False
            except Exception as e:
                print "There was an error in obtaining Globus token. Please check username or your password"
                sys.stderr.write(str(e) + "\n")

        if b_will_exit:
            print "Thank you for setup. Please run client again"
            exit(1)

        self.write_cfg_file()


    def write_cfg_file(self):
        with open(self.config_file_name, 'w') as configfile:
            self.config.write(configfile)

    def get_cfg_field(self, field, namespace='Default'):
        try:
            return str(self.config.get(namespace, field))
        except:
            print "%s is not defined"% field
            return "None"

    def gd_init_catalog(self,datasetClient):
        catalog_id = self.get_cfg_field('catalog',namespace='GeoDataspace')
        if  catalog_id == "None":
            nr_tries = 0
            while (nr_tries<3 and catalog_id == "None"):
                catalog_name = raw_input("Please provide catalog name > ")
                # Show the data to user and get catalog_name from user
                catalog_json = get_catalog_by_name(datasetClient,catalog_name)
                if catalog_json is not None:
                    catalog_id = str(catalog_json.get('id',None))
                    self.config['GeoDataspace']['catalog'] = catalog_id
                else:
                    print "Could not find catalog with name containing '%s'"%catalog_name
                nr_tries += 1

            self.write_cfg_file()
        return catalog_id


if __name__ == '__main__':
    # Read configuration file
    cfg = GDConfig()
    
    ## Init a datasetclient
    datasetClient = DatasetClient(cfg.config['Default']['goauth-token'],cfg.config['Default']['URL'])
    
    ## If datasetclient is not None then connection between client and server established.
    if datasetClient is None:
        print "cannot obtain a valid dataset client"
        exit(1)

   
    mycatalog_id = cfg.gd_init_catalog(datasetClient)
    #print "mycatalog_id=",mycatalog_id

    dataset_list = get_last_datasets(datasetClient,mycatalog_id)
    dataset_dict = {key:{} for key in dataset_list}

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
        'geounit':{'start':dataset_dict, 'delete':{}},
        'add_member':{},
        'annotate':{'geounit':
                         {'geoprop1':{}, 'prop2':{}, 'fluid':{}
                          },
                     'member':{}
                     },
        'stop':{}
    }
    readline.set_completer(BufferAwareCompleter(completer_suggestions).complete)

    # Use the tab key for completion
    readline.parse_and_bind('tab: complete')


    geounit_name = UNDEFINED
    geounit_id = None
    while True :
        raw_cmd = raw_input(geounit_name+" > ")
        cmd_to_run = raw_cmd
        cmd_splitted = SafeList(raw_cmd.split())

        if cmd_splitted.get(0) == "stop":
            break
        elif cmd_splitted.get(0)=="geounit":
            geounit_name, geounit_id, err_message = parse_cmd_geounit(cmd_splitted, mycatalog_id, geounit_id, datasetClient)
            if err_message != "":
                print err_message

        elif cmd_splitted.get(0)=="annotate":
            parse_cmd_annotate(cmd_splitted, geounit_id, mycatalog_id, datasetClient)

        elif cmd_splitted.get(0)=="add_member":
            parse_cmd_add_member(cmd_splitted,mycatalog_id, geounit_id, datasetClient)
        else:
            # any bash command that we want to pass to the system
            #print "any cmd"
            run_command(cmd_to_run)
    print "done"



