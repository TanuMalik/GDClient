'''
Created on Apr 27, 2015

@author: wozniak
'''

class Queries:
    '''
    Advanced queries for Globus Catalog
    '''

    def __init__(self, client, catalog_id):
        self.client = client
        self.catalog_id = catalog_id
    
    '''
    Obtain dict of all files in catalog mapped to dataset ID
    ''' 
    def list_files(self, catalog):
        result = {}
        _, datasets = self.client.get_datasets(self.catalog_id)
        for ds in datasets:
            dataset_id = ds['id']
            _, members = self.client.get_members(self.catalog_id, 
                                                 dataset_id)
            for m in members:
                data_uri = m['data_uri']
                result[data_uri] = dataset_id 
        return result
