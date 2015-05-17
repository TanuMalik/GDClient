#!/usr/bin/env python

import sys

from globusonline.catalog.client.goauth import get_access_token
from globusonline.catalog.client.dataset_client import DatasetClient

BASE_URL = "https://catalog-alpha.globuscs.info/service/dataset"

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: %s catalog_id" % sys.argv[0]
        sys.exit(1)

    catalog_id = sys.argv[1]

    # Prompt for username/password. Note that you can also pass them in
    # directly for non-interactive use.
    goauth_result = get_access_token()
    client = DatasetClient(goauth_result.token, base_url=BASE_URL)
    _, data = client.get_datasets(catalog_id)
    for ds in data:
        print ds
        print "==== %s ====" % ds["name"]
        for k, v in ds.iteritems():
            if k == "name":
                continue
            print "%s: %s" % (k, v)
