import sys
import json
import traceback
from optparse import OptionParser

from globusonline.catalog.client.operators import Op
from globusonline.catalog.client.rest_client import RestClientError
from globusonline.catalog.client.dataset_client import DatasetClient

def get_catalogs(client):
    try:
        _, catalog_list = client.get_catalogs()
        if not show_output:
            return True
        if print_text:
            print "============================================================"
            print "*More detailed catalog information available in JSON format*"
            print 'ID) Catalog Name - [Owner] - Catalog Description'
            print "============================================================"
        for catalog in catalog_list:
            if print_text:
                print format_catalog_text(catalog)
            else:
                print json.dumps(catalog_list)
    except KeyError:
        # print e
        return False
    return True
    """Get a catalog who's name matches selected criteria.
      If more catalogs are found, it returns the first found.

       @return: catalog dictionary
    """
def get_catalog_by_name(client, name=".*"):

     r, catalogs = client.get_catalogs()
     match_criteria = r".*%s.*" % name
     catalogs_found = filter(lambda catalog: re.search(match_criteria, catalog["config"]["name"]) is not None, catalogs)

     catalogs_number = len(catalogs_found)
     if catalogs_number > 1:
         print "Found %d catalogs matching serch criteria <%s>." % (catalogs_number, name)

     if catalogs_number>0:
         selected_catalog = catalogs_found[0]
     else:
         selected_catalog = None

     return selected_catalog

