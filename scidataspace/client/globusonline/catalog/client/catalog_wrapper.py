#!/usr/bin/env python
from globusonline.catalog.client.goauth import get_access_token
from globusonline.catalog.client.dataset_client import DatasetClient
from globusonline.transfer.api_client import TransferAPIClient, Transfer
import re
import collections
import os

class CatalogWrapper:
## Catalog wrapper wraps the features of the Globus Catalog API, and includes an interface to interact with Globus Transfer 

    def __init__(self, username=None, password=None,token=None, token_file=None):
        self.username        = username
        self.password        = password
        self.token           = token
        self.token_file      = token_file  
        self.members         = ''
        self.transfer_details = []
        self.catalog_base_url  = "https://catalog-alpha.globuscs.info/service/dataset"
        #Transfer Related Variables
        self.transfer_base_url = "https://transfer.test.api.globusonline.org/v0.10"
        self.destination_endpoint = ''
        self.destination_path   ='~'
        self.submission_id = ''
        self.transfer = ''
        self.active_endpoints = ''
        self.transfer_queue = ''
        #Client Variables
        self.catalogClient  = ''     #client for interfacing with Globus Catalog
        #Debug Variables
        self.debug = False
        if os.getenv("GCAT_DEBUG", "0") is "1":
            print "enabling debug"
            self.debug = True
        self.debug_list = []
        
        self.GO_authenticate()

        if self.debug is True:
            self.debug_list.append('')
        
    def __del__(self):
        if self.debug is True:
            debug_str = '\n'.join(self.debug_list)
            print debug_str
            file = open('catalog_wrapper-debug.txt','w')
            file.write(debug_str)
        else:
            pass

    @staticmethod
    def default_token_file():
        return os.getenv('HOME')+"/.ssh/gotoken.txt"

    #Member Functions      
    def GO_authenticate(self):
        # Grab the access token, and store the token itself in GO_ACCESS_TOKEN
        tmpToken = ''

        if self.username is not None and self.password is not None:
            tmpToken = get_access_token(self.username, self.password)
            self.token = tmpToken.token
        elif self.token_file is not None:
            try:
                file = open(self.token_file,'r')
                self.token = file.read()
                self.username = self.token.split('|')[0][3:] #read the username from the token file which is split by | and listed after un= in the first string
            except Exception:
                self.create_token_file()
            else:
                pass            
        else:
            tmpToken = get_access_token()
            self.token = tmpToken.token  
            self.username = self.token.split('|')[0][3:] #read the username from the token file which is split by | and listed after un= in the first string          

        if(self.token):
            self.catalogClient = DatasetClient(self.token.strip(), self.catalog_base_url)
            self.transferClient = TransferAPIClient(self.username, goauth=self.token, base_url=self.transfer_base_url)

    def check_authenticate(self):
        #This could be a more robust check if necessary
        if self.token != '':
            return True
        else:
            return False

    def create_token_file(self):
        tmpToken = get_access_token()
        file = open(self.token_file,'w')
        file.write(tmpToken.token)
        self.token = tmpToken.token
        return True  

    def delete_token_file(self):
        try:
            os.remove(self.token_file)
        except Exception, e:
            print e
            return False
        print 'Authentication Token Removed from:',self.token_file
        return True      

    ##Transfer/Catalog Interfacing    
    def set_destination_endpoint(self, destination_endpoint=None):
        self.destination_endpoint = destination_endpoint
        return True
    
    def transfer_members(self, catalog_id, dataset_id, local_path=None):
        _,self.members = self.catalogClient.get_members(catalog_id,dataset_id)
        self.transfer_details = self.extract_transfer_details(self.members)
        self.transfer_queue = self.group_transfers(self.transfer_details)
        self.activate_endpoints(self.transfer_details)

        for bundle in self.transfer_queue:
            self.debug_list.append('==Unwrapping transfer bundle and creating transfer instance/ID %s => %s=='%(bundle[0]['endpoint'],self.destination_endpoint))
            #code, reason, result = self.transferClient.transfer_submission_id()
            #submission_id = result["value"]
            #transfer_object = Transfer(submission_id, bundle[0]['endpoint'], self.destination_endpoint)     
            for transfer in bundle:
                self.debug_list.append('==Starting transfer of %s @%s from %s to %s %s=='%(transfer['type'],transfer['location'],
                                                                          transfer['endpoint'],self.destination_endpoint, 
                                                                          self.destination_path))
                if transfer['type'] == 'file':
                    self.debug_list.append('==Transferring File==')
                    #transfer_object.add_item(trans['location'], self.destination_path)
                elif transfer['type'] == 'directory':
                    self.debug_list.append('==Transferring Directory -- Adding Recursion==')
                    #transfer_object.add_item(trans['location'], self.destination_path, recursive=True)
                else:
                    self.debug_list.append('==Uncaught Type==')
            # status, reason, result = self.transferClient.transfer(transfer_object)
            # task_id = result["task_id"]
            # self.debug_list.append('==TRANSFER TASK ID: %s=='%(result["task_id"]))
            # status, reason, result = self.transferClient.task(task_id)
            # self.debug_list.append('==TRANSFER RESULT: %s=='%(result["status"]))
            #transfer the bundle
        return True

    def extract_transfer_details(self, member_list):
        #get the endpoints and data locations from the data_uri and store them in transfer_details    
        transfer_details = []
        self.debug_list.append('==Extracting Transfer Details==')
        for member in member_list:
            tmpDict = {}
            match = re.findall('globus://([\w#]+)/([\w/~._-]+)',member['data_uri'])
            if match != []:
                tmpDict['endpoint'] = match[0][0]
                tmpDict['location'] = match[0][1]
                tmpDict['type']     = member['data_type']
                transfer_details.append(tmpDict)
        return transfer_details

    def group_transfers(self, transfer_details):
        grouped_transfers = collections.defaultdict(list)
        self.debug_list.append('==Grouping Transfers==')
        for detail in transfer_details:
            grouped_transfers[detail['endpoint']].append(detail)
        return grouped_transfers.values()

    def activate_endpoints(self, transfer_details):
        unique_endpoints = set()
        unique_endpoints.add(self.destination_endpoint)
        for detail in transfer_details:
            unique_endpoints.add(detail['endpoint'])
        for endpoint in unique_endpoints:
            self.debug_list.append("==Activating Endpoint"+endpoint+"==")
            status, message, data = self.transferClient.endpoint_autoactivate(endpoint)
            #check data["code"] value starts with activation failed, raise an exception
            #test with shared endpoints created on goep1 goep2


