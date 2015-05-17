#!/usr/bin/env python

BASE_URL = "https://catalog-alpha.globuscs.info/service/dataset"

from globusonline.catalog.client.goauth import get_access_token
from globusonline.catalog.client.dataset_client import DatasetClient


if __name__ == "__main__":
    # Prompt for username/password. Note that you can also pass them in
    # directly for non-interactive use.
    goauth_result = get_access_token()
    client = DatasetClient(goauth_result.token, base_url=BASE_URL)
    _, data = client.get_catalogs()
    for cat in data:
        print cat
        print "==== %s ====" % cat["config"]["name"]
        for k, v in cat["config"].iteritems():
            if k == "name":
                continue
            print "%s: %s" % (k, v)
