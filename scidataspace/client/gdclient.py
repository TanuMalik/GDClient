__author__ = 'cristian'

import os, sys

import code
import logging
import readline
import requests
import atexit

import configparser

from leveldb import LevelDB, LevelDBError
from globusonline.catalog.client.dataset_client import DatasetClient
from globusonline.catalog.client.goauth import get_access_token
from scidataspace.client.completer import BufferAwareCompleter
from scidataspace.client.query_dataset_client import get_catalogs, get_catalog_by_name, get_last_datasets

from scidataspace.client.commands.util import UNDEFINED, SafeList, run_command, is_geounit_selected
from scidataspace.client.commands.geounit import parse_cmd_geounit

from scidataspace.client.commands.annotate import parse_cmd_annotate
from scidataspace.client.commands.add_member import parse_cmd_add_member
from scidataspace.client.commands.package import parse_cmd_package
from scidataspace.client.commands.transfer import parse_cmd_transfer
from scidataspace.client.commands.track import parse_cmd_track

import warnings
warnings.filterwarnings("ignore")

global datasetClient

def init_DatasetClient(goauth_token,BASE_URL):
    datasetClient = DatasetClient(goauth_token, BASE_URL)
    if datasetClient is None:
        print "cannot obtain a valid dataset client"
        exit(1)

class GDConfig:
    config_file_name = None
    config = None

    ## Read the config.ini file and check if URL is set
    ## if not ask to set it and exit
    ## if username is none ask for username and store it config.ini.
    ## Next time the client is run, read username from config.ini
    ## If Globus token is none, Obtain Globus token and store it, else proceed
    ## Return config
    def __init__(self):
        self.config_file_name = os.path.join(os.path.expanduser("~"),'.gdclient','config.ini')
        self.config = configparser.ConfigParser()
        try:
            self.config.read_file(open(self.config_file_name))
        except:
            self.config['Default'] = {'url': "https://ec2-54-84-57-85.compute-1.amazonaws.com/service/dataset",
                                   'uname' : 'None',
                                   'goauth-token' : 'None'}
            self.config['GLOBUS'] = {
                'local-endpoint': 'tanum#gdclient-hydro',
                'local-folder': '~/.gdclient/docker_images',
                'globus-local-folder': '/docker_image/',
                'remote-endpoint': 'cvlaescx#server_240',
                'globus-remote-folder': '/~/globus-stuff/users/cvlaescx/'
                }
            self.config['GeoDataspace'] = {'catalog' : 11}
            self.write_cfg_file()

        if self.get_cfg_field('URL') == "None":
            print "GeoDataspace URL is not set in %s"%self.config_file_name
            exit(1)

        b_will_exit = False
        if  self.get_cfg_field('uname') == "None":
            uname = raw_input("Please provide user name > ")
        else:
            uname = self.get_cfg_field('uname')

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
    home_folder = os.path.expanduser("~")
    dot_gdclient_folder = os.path.join(home_folder,'.gdclient')
    if not os.path.exists(dot_gdclient_folder):
        os.makedirs(dot_gdclient_folder)

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

    levelDB_local_database = os.path.join(dot_gdclient_folder,".gdclient_levelDB")
    docker_image_id = None
    ## Add history
    histfile = os.path.join(dot_gdclient_folder,".gd_history")
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
        'track':{},
        'transfer':{},
        'package':{'provenance':
                         {'level':{'individual':{}, 'collaboration':{}, 'community':{}}
                          },
                     'level':{'individual':{}, 'collaboration':{}, 'community':{}},
                     'list':{},
                     'add':{},
                     'delete':{},
                     },
        'annotate':{'geounit':
                         {'geoprop1':{}, 'prop2':{}, 'fluid':{}
                          },
                     'member':{}
                     },
        'stop':{}
    }
    gd_client_special_string = '--'
    print ("gdclient commands start with  "+gd_client_special_string)
    comp=BufferAwareCompleter(completer_suggestions,gd_client_special_string)
    readline.set_completer_delims(' \t\n')
    if 'libedit' in readline.__doc__:
        readline.parse_and_bind("bind ^I rl_complete")
    else:
        readline.parse_and_bind("tab: complete")
    readline.set_completer(comp.complete)

    geounit_name = UNDEFINED
    geounit_id = None
    while True :
        raw_cmd = raw_input(geounit_name+" > ")
        cmd_to_run = raw_cmd.strip()
        cmd_splitted = SafeList(raw_cmd.split())
        first_command = cmd_splitted.get(0,'')

        if first_command.upper() in ['--STOP','X']:
            break
        elif first_command == "--geounit":
            geounit_name, geounit_id, err_message = parse_cmd_geounit(cmd_splitted, mycatalog_id, geounit_id, datasetClient)
            if err_message != "":
                print err_message

        elif first_command == "--track":
            if is_geounit_selected(geounit_id):
                parse_cmd_track(cmd_splitted)

        elif first_command == "--transfer":
            if is_geounit_selected(geounit_id):
                parse_cmd_transfer(cmd_splitted,docker_image_id, cfg)

        elif first_command == "--package":
            new_image_id = parse_cmd_package(cmd_splitted, mycatalog_id, geounit_id, datasetClient, db, cfg)
            if new_image_id:
                docker_image_id = new_image_id

        elif first_command in ["--annotate", "--add_member"]:
            locals()["parse_cmd_"+first_command[2:]](cmd_splitted, mycatalog_id, geounit_id, datasetClient, db)

        elif first_command == "cd":
            try:
                os.chdir(cmd_splitted.get(1,home_folder))
            except Exception as e:
                sys.stderr.write(str(e) + "\n")

        else:
            # any bash command that we want to pass to the system
            run_command(cmd_to_run)
    print "done"



