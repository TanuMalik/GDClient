"""
Client for the Dataset REST API.
"""
import json
import urllib
import re

from globusonline.catalog.client import rest_client
from globusonline.catalog.client.rest_client import urlquote
from globusonline.catalog.client.operators import Op, build_selector

DEFAULT_BASE_URL = "https://localhost/service/dataset"


class DatasetClient(rest_client.GoauthRestClient):
    """
    Note: all helper methods return the response object followed by the
    body, parsed from json if possible. The return values listed below in the
    docstrings just describe the body.

    On failure the method will raise RestClientError, and the body will
    typically be plain text (need to fix this in tagfielr).
    """
    def __init__(self, goauth_token, base_url=DEFAULT_BASE_URL, max_attempts=1,
                 parse_json=True, log_requests=False):
        super(DatasetClient, self).__init__(goauth_token, base_url,
                                            max_attempts, parse_json,
                                            log_requests)

    def create_catalog(self, catalog_dict=None, **kw):
        """Create a catalog with the given name and addional attributes
        specified in a dictionary and/or keyword parameters. Note that
        most of the attributes of interest (including name and description)
        must be in a dictionary mapped from the 'config' key:

         client.create_catalog(config=dict(name=..., description=...))

        @return: dictionary of catalog properties (most notably 'id')
        """
        if catalog_dict is None:
            catalog_dict = kw
        else:
            catalog_dict.update(kw)
        body = json.dumps(catalog_dict)
        return self._request("POST", "/catalog", body)

    def delete_catalog(self, catalog_id):
        """Delete the specified catalog."""
        return self._request("DELETE", "/catalog/id=%s"
                                       % urlquote(catalog_id))

    def get_catalogs(self):
        """Get a list of all catalogs.

        @return: list of catalog dictionaries
        """
        return self._request("GET", "/catalog")

    def create_dataset(self, catalog_id, dataset):
        """Create a dataset with the given name and addional attributes
        specified in keyword parameters.

        @param catalog_id: catalog to create the dataset in
        @param dataset: dictionary of dataset properties

        @return: dictionary of dataset properties (most notably 'id')
        """
        return self._request("POST", "/catalog/id=%s/dataset"
                                     % urlquote(catalog_id),
                             json.dumps(dataset))

    def delete_dataset(self, catalog_id, dataset_id):
        """Delete the specified dataset."""
        return self._request("DELETE", "/catalog/id=%s/dataset/id=%s"
                                       % (urlquote(catalog_id),
                                          urlquote(dataset_id)))

    def get_datasets(self, catalog_id, last_id=None, limit=100,
                     selector_list=None):
        """Get a paged list of datasets the user has permission to view.
        Paging is done based on last id from the previous page, not numeric
        offset.

        @return: list of dataset dictionaries
        """
        params = dict(limit=limit)
        qs = urllib.urlencode(params)
        if selector_list is None:
            selector_list = []
        if last_id is not None:
            selector_list += [("id", Op.GT, last_id)]
        query = build_selector(selector_list)
        return self._request("GET", "/catalog/id=%s/dataset/%s?%s"
                                    % (urlquote(catalog_id), query, qs))

    def get_dataset_acl(self, catalog_id, dataset_id):
        path = "/catalog/id=%s/dataset/id=%s/acl" % (
                    urlquote(catalog_id), urlquote(dataset_id))
        return self._request("GET", path)

    def add_dataset_acl(self, catalog_id, dataset_id, access_rules):
        path = "/catalog/id=%s/dataset/id=%s/acl" % (
                    urlquote(catalog_id), urlquote(dataset_id))
        return self._request("POST", path, json.dumps(access_rules))

    def get_dataset_access_rule(self, catalog_id, dataset_id, principal_type,
                                principal):
        if principal_type not in ("user", "group"):
            raise ValueError("principal_type must be 'user' or 'group'")
        path = "/catalog/id=%s/dataset/id=%s/acl/%s/%s" % (
                    urlquote(catalog_id), urlquote(dataset_id),
                    principal_type, urlquote(principal))
        return self._request("GET", path)

    def delete_dataset_access_rule(self, catalog_id, dataset_id,
                                   principal_type, principal):
        if principal_type not in ("user", "group"):
            raise ValueError("principal_type must be 'user' or 'group'")
        path = "/catalog/id=%s/dataset/id=%s/acl/%s/%s" % (
                    urlquote(catalog_id), urlquote(dataset_id),
                    principal_type, urlquote(principal))
        return self._request("DELETE", path)

    def get_dataset_annotations(self, catalog_id, dataset_id=None,
                                annotation_list=None, selector_list=None,
                                **params):
        """Get a list of annotations on the matching datasets in the specified
        catalog. Pass either dataset_id for a single dataset, a
        selector_list for complex searching, or neither to get all datasets
        in the catalog.

        @param catalog_id: catalog containing the dataset
        @param dataset_id: dataset to return annotation for
        @param selector_list: list of selector tuples, as an alternative to
                              dataset_id
        @param annotation_list: optional list of annotation names to return;
                                defaults to all annotations.
        """
        if selector_list is not None and dataset_id is not None:
            raise ValueError("specify one of selector_list or dataset_id")
        if selector_list is None:
            if dataset_id is None:
                selector_list = ["name"]
            else:
                selector_list = [("id", Op.EQUAL, dataset_id)]
        query = build_selector(selector_list)
        path = "/catalog/id=%s/dataset/%s/annotation" %(
                urlquote(catalog_id), query)
        if annotation_list is not None:
            path = "%s/%s" % (path,
                              ";".join(urlquote(x) for x in annotation_list))
        if params:
            path = "%s?%s" % (path, urllib.urlencode(params))
        return self._request("GET", path)

    def get_dataset_annotation_ranges(self, catalog_id, dataset_id=None,
                                      annotation_list=None,
                                      selector_list=None):
        return self.get_dataset_annotations(catalog_id,
                                            dataset_id=dataset_id,
                                            selector_list=selector_list,
                                            annotation_list=annotation_list,
                                            range="values",
                                            versions="latest")

    def add_dataset_annotations(self, catalog_id, dataset_id,
                                annotations_dict):
        path = "/catalog/id=%s/dataset/id=%s/annotation" %(
                urlquote(catalog_id), urlquote(dataset_id))
        return self._request("POST", path, json.dumps(annotations_dict))

    def delete_dataset_annotation(self, catalog_id, dataset_id,
                                 annotation_name, annotation_value=None):
        """Delete the given annotation and/or value from the specified
        dataset."""
        if annotation_value is not None:
            query = "%s=%s" % (urlquote(annotation_name),
                               urlquote(annotation_value))
        else:
            query = urlquote(annotation_name)

        path = ("/catalog/id=%s/dataset/id=%s/annotation/%s"
                % (urlquote(catalog_id), urlquote(dataset_id), query))
        return self._request("DELETE", path)

    def create_member(self, catalog_id, dataset_id, member):
        """Helper for creating a single member using the standard bulk
        interface."""
        return self.create_members(catalog_id, dataset_id, [member])

    def create_members(self, catalog_id, dataset_id, members):
        """Create members in the given dataset.

        @param catalog_id: catalog to create the member in
        @param member: list of dictionaries of member properties

        @return: list of dictionary of member properties (most notably 'id')
        """
        return self._request("POST", "/catalog/id=%s/dataset/id=%s/member"
                                     % (urlquote(catalog_id),
                                        urlquote(dataset_id)),
                             json.dumps(members))

    def delete_member(self, catalog_id, dataset_id, member_id):
        """Delete the specified member."""
        return self._request("DELETE",
                             "/catalog/id=%s/dataset/id=%s/member/id=%s"
                              % (urlquote(catalog_id),
                                 urlquote(dataset_id),
                                 urlquote(member_id)))

    def get_members(self, catalog_id, dataset_id, last_id=None, limit=100, selector_list=None):
        """Get a list of all members the user has permission to view.
        Paging is done based on last id from the previous page, not numeric
        offset.

        @return: list of member dictionaries
        """
        params = dict(limit=limit)
        qs = urllib.urlencode(params)
        if selector_list is None:
            selector_list = []
        if last_id is not None:
            selector_list += [("id", Op.GT, last_id)]
        query = build_selector(selector_list)
        return self._request("GET", "/catalog/id=%s/dataset/id=%s/member/%s?%s" % (urlquote(catalog_id), urlquote(dataset_id),query, qs))

    def create_annotation_def(self, catalog_id, annotation_name,
                              value_type, multivalued=False, unique=False):
        body = dict(value_type=value_type,
                    multivalued=multivalued,
                    unique=unique)
        return self._request("PUT", "/catalog/id=%s/annotation_def/%s"
                                    % (urlquote(catalog_id),
                                       urlquote(annotation_name)),
                             json.dumps(body))

    def get_annotation_defs(self, catalog_id):
        """Get a list of all annotations defined for the catalog.

        @return: list of annotation def dictionaries
        """
        return self._request("GET", "/catalog/id=%s/annotation_def"
                                    % (urlquote(catalog_id)))

    def delete_annotation_def(self, catalog_id, annotation_name):
        return self._request("DELETE", "/catalog/id=%s/annotation_def/%s"
                                    % (urlquote(catalog_id),
                                       urlquote(annotation_name)))

    def add_member_annotations(self, catalog_id, dataset_id, member_id,
                               annotation_dict):
        path = ("/catalog/id=%s/dataset/id=%s/member/id=%s/annotation"
                % (urlquote(catalog_id), urlquote(dataset_id),
                   urlquote(member_id)))
        body = json.dumps(annotation_dict)
        return self._request("POST", path, body)

    def get_member_annotations(self, catalog_id, dataset_id, member_id,
                               annotation_list=None):
        path = ("/catalog/id=%s/dataset/id=%s/member/id=%s/annotation"
                % (urlquote(catalog_id), urlquote(dataset_id),
                   urlquote(member_id)))
        if annotation_list:
            # TODO: handle tag=value patterns, useful for multivalued
            # tags
            pattern = ";".join(urlquote(x) for x in annotation_list)
            path = "%s/%s" % (path, pattern)
        return self._request("GET", path)

    def get_all_member_annotations(self, catalog_id, dataset_id, member_list, annotation_list=None, limit=100):
        path = ("/catalog/id=%s/dataset/id=%s/member"
                % (urlquote(catalog_id), urlquote(dataset_id)))
        print path

        params = dict(limit=limit)
        qs = urllib.urlencode(params)

        if annotation_list:
            # TODO: handle tag=value patterns, useful for multivalued
            # tags
            member_pattern = ";".join(urlquote(x) for x in annotation_list)
            annotation_pattern = ";".join(urlquote(y) for y in annotation_list)
            path = "%s/%s/annotation/%s?%s" % (path, member_pattern, annotation_pattern,qs)
            print path
        return self._request("GET", path)

    def delete_member_annotation(self, catalog_id, dataset_id, member_id,
                                 annotation_name, annotation_value=None):
        """Delete the given annotation and/or value from the specified
        member."""
        if annotation_value is not None:
            query = "%s=%s" % (urlquote(annotation_name),
                               urlquote(annotation_value))
        else:
            query = urlquote(annotation_name)

        path = ("/catalog/id=%s/dataset/id=%s/member/id=%s/annotation/%s"
                % (urlquote(catalog_id), urlquote(dataset_id),
                   urlquote(member_id), query))
        return self._request("DELETE", path)


if __name__ == "__main__":
    # For testing with ipython
    import sys
    if len(sys.argv) < 3:
        print "Usage: %s goauth_token [base_url]" % sys.argv[0]
        sys.exit(1)
    goauth_token = sys.argv[1]
    if len(sys.argv) > 2:
        base_url = sys.argv[2]
    else:
        base_url = DEFAULT_BASE_URL
    client = DatasetClient(goauth_token, base_url)
