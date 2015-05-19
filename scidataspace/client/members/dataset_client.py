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

    def get_catalog_by_name(self, name=".*"):
        """Get a catalog who's name matches selected criteria.
        If more catalogs are found, it returns the first found.

        @return: catalog dictionary
        """

        r, catalogs = self._request("GET", "/catalog")
        match_criteria = r".*%s.*" % name
        catalogs_found = filter(lambda catalog: re.search(match_criteria, catalog["config"]["name"]) is not None, catalogs)
        """
        catalogs_found=[]

        for catalog in catalogs:
            matches =
            if matches is not None:
                catalogs_found.append(catalog)
"""
        catalogs_number = len(catalogs_found)
        if catalogs_number > 1:
            print "Found %d catalogs matching serch criteria <%s>." % (catalogs_number, name)

        if catalogs_number>0:
            selected_catalog = catalogs_found[0]
        else:
            selected_catalog = None

        return selected_catalog

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

    def get_members(self, catalog_id, dataset_id, last_id=None, 
                     limit=100, selector_list=None):
        """Get a list of all members the user has permission to view.
        Paging is done based on last id from the previous page, not numeric
        offset.

        @return: list of member dictionaries
        """
        params = dict(limit=limit)
        qs = urllib.urlencode(params)
        if selector_list is None:
            selector_list = [( "id", Op.GT, 0 )]
        if last_id is not None:
            selector_list += [("id", Op.GT, last_id)]
        selector_list += [("data_uri", Op.NOT_EQUAL, "None")]
        selector_list += [("data_type", Op.NOT_EQUAL, "None")]

        query = build_selector(selector_list)
        return self._request("GET", "/catalog/id=%s/dataset/id=%s/member/%s?%s"
                                    % (urlquote(catalog_id),
                                        urlquote(dataset_id),
                                        query, qs))

    #def get_members(self, catalog_id, dataset_id):
    #    """Get a list of all members the user has permission to view.
    #
    #    @return: list of member dictionaries
    #    """
    #    return self._request("GET", "/catalog/id=%s/dataset/id=%s/member"
    #                                % (urlquote(catalog_id),
    #                                   urlquote(dataset_id)))

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


#    """
#    This should not be visible to users.
#    Maybe we need to remove this code
#
#    def delete_change(self, catalog_id, dataset_id, member_id, change_id):
#        """Delete the specified change."""
#        return self._request("DELETE",
#                             "/catalog/id=%s/dataset/id=%s/change/id=%s"
#                              % (urlquote(catalog_id),
#                                 urlquote(dataset_id),
#                                 urlquote(change_id)))
#    """

    def get_changes(self, catalog_id, dataset_id, last_id=None,
                     limit=100, selector_list=None):
        """Get a list of all changes the user has permission to view.
        Paging is done based on last id from the previous page, not numeric
        offset.

        @return: list of change dictionaries
        """
        params = dict(limit=limit)
        qs = urllib.urlencode(params)
        if selector_list is None:
            selector_list = [( "id", Op.GT, 0 )]
        if last_id is not None:
            selector_list += [("id", Op.GT, last_id)]
        query = build_selector(selector_list)
        return self._request("GET", "/catalog/id=%s/dataset/id=%s/change/%s?%s"
                                    % (urlquote(catalog_id),
                                        urlquote(dataset_id),
                                        query, qs))

    def create_edge(self, catalog_id, dataset_id, member_id, edge):
        """Helper for creating a single edge using the standard bulk
        interface."""
        return self.create_edges(catalog_id, dataset_id, member_id, [edge])

    def create_edges(self, catalog_id, dataset_id, member_id, edges):
        """Create edges in the given dataset.

        @param catalog_id: catalog to create the edge in
        @param edge: list of dictionaries of edge properties

        @return: list of dictionary of edge properties (most notably 'id')
        """
        return self._request("POST", "/catalog/id=%s/dataset/id=%s/member/id=%s/edge"
                                     % (urlquote(catalog_id),
                                        urlquote(dataset_id),
                                        urlquote(member_id)),
                             json.dumps(edges))

    def delete_edge(self, catalog_id, dataset_id, member_id, edge_id):
        """Delete the specified edge."""
        return self._request("DELETE",
                             "/catalog/id=%s/dataset/id=%s/member/id=%s/edge/id=%s"
                              % (urlquote(catalog_id),
                                 urlquote(dataset_id),
                                 urlquote(member_id),
                                 urlquote(edge_id)))

    def get_edges(self, catalog_id, dataset_id, member_id, last_id=None,
                     limit=100, selector_list=None):
        """Get a list of all edges the user has permission to view.
        Paging is done based on last id from the previous page, not numeric
        offset.

        @return: list of edge dictionaries
        """
        params = dict(limit=limit)
        qs = urllib.urlencode(params)
        if selector_list is None:
            selector_list = [( "id", Op.GT, 0 )]
        if last_id is not None:
            selector_list += [("id", Op.GT, last_id)]
        #selector_list += [("data_uri", Op.NOT_EQUAL, "None")]
        query = build_selector(selector_list)
        return self._request("GET", "/catalog/id=%s/dataset/id=%s/member/id=%s/edge/%s?%s"
                                    % (urlquote(catalog_id),
                                        urlquote(dataset_id),
                                        urlquote(member_id),
                                        query, qs))

    def create_transformation(self, catalog_id, dataset_id, transformation):
        """Create transformations in the given dataset.

        @param catalog_id: catalog to create the transformation in
        @param dataset_id: dataset to create the transformation for
        @param transformation: dictionary of transformation properties

        @return: dictionary of transformation properties (most notably 'id')
        """
        return self._request("POST", "/catalog/id=%s/dataset/id=%s/transformation"
                                     % (urlquote(catalog_id),
                                        urlquote(dataset_id)),
                             json.dumps(transformation))

    def delete_transformation(self, catalog_id, dataset_id, transformation_id):
        """Delete the specified transformation."""
        return self._request("DELETE",
                             "/catalog/id=%s/dataset/id=%s/transformation/id=%s"
                              % (urlquote(catalog_id),
                                 urlquote(dataset_id),
                                 urlquote(transformation_id)))

    def get_transformations(self, catalog_id, dataset_id, last_id=None,
                     limit=100, selector_list=None):
        """Get a list of all transformations the user has permission to view.
        Paging is done based on last id from the previous page, not numeric
        offset.

        @return: list of transformation dictionaries
        """
        params = dict(limit=limit)
        qs = urllib.urlencode(params)
        if selector_list is None:
            selector_list = [( "id", Op.GT, 0 )]
        if last_id is not None:
            selector_list += [("id", Op.GT, last_id)]
        selector_list += [("name", Op.NOT_EQUAL, "None")]
        query = build_selector(selector_list)
        return self._request("GET", "/catalog/id=%s/dataset/id=%s/transformation/%s?%s"
                                    % (urlquote(catalog_id),
                                        urlquote(dataset_id),
                                        query, qs))


#    def get_transformations(self, catalog_id, dataset_id):
#        """Get a list of all transformations the user has permission to view.
#
#        @return: list of transformation dictionaries
#        """
#        return self._request("GET", "/catalog/id=%s/dataset/id=%s/transformation"
#                                    % (urlquote(catalog_id),
#                                       urlquote(dataset_id)))

    def add_transformation_annotations(self, catalog_id, dataset_id, transformation_id,
                               annotation_dict):
        path = ("/catalog/id=%s/dataset/id=%s/transformation/id=%s/annotation"
                % (urlquote(catalog_id), urlquote(dataset_id),
                   urlquote(transformation_id)))
        body = json.dumps(annotation_dict)
        return self._request("POST", path, body)

    def get_transformation_annotations(self, catalog_id, dataset_id, transformation_id,
                               annotation_list=None):
        path = ("/catalog/id=%s/dataset/id=%s/transformation/id=%s/annotation"
                % (urlquote(catalog_id), urlquote(dataset_id),
                   urlquote(transformation_id)))
        if annotation_list:
            # TODO: handle tag=value patterns, useful for multivalued
            # tags
            pattern = ";".join(urlquote(x) for x in annotation_list)
            path = "%s/%s" % (path, pattern)
        return self._request("GET", path)

    def delete_transformation_annotation(self, catalog_id, dataset_id, transformation_id,
                                 annotation_name, annotation_value=None):
        """Delete the given annotation and/or value from the specified
        transformation."""
        if annotation_value is not None:
            query = "%s=%s" % (urlquote(annotation_name),
                               urlquote(annotation_value))
        else:
            query = urlquote(annotation_name)

        path = ("/catalog/id=%s/dataset/id=%s/transformation/id=%s/annotation/%s"
                % (urlquote(catalog_id), urlquote(dataset_id),
                   urlquote(transformation_id), query))
        return self._request("DELETE", path)

    def create_provenance(self, catalog_id, dataset_id, provenance):
        """Create provenances in the given dataset.

        @param catalog_id: catalog to create the provenance in
        @param dataset_id: dataset to create the provenance for
        @param provenance: dictionary of provenance properties

        @return: dictionary of provenance properties (most notably 'id')
        """
        return self._request("POST", "/catalog/id=%s/dataset/id=%s/provenance"
                                     % (urlquote(catalog_id),
                                        urlquote(dataset_id)),
                             json.dumps(provenance))

    def delete_provenance(self, catalog_id, dataset_id, provenance_id):
        """Delete the specified provenance."""
        return self._request("DELETE",
                             "/catalog/id=%s/dataset/id=%s/provenance/id=%s"
                              % (urlquote(catalog_id),
                                 urlquote(dataset_id),
                                 urlquote(provenance_id)))

    def get_provenances(self, catalog_id, dataset_id, last_id=None,
                     limit=100, selector_list=None):
        """Get a list of all provenances the user has permission to view.
        Paging is done based on last id from the previous page, not numeric
        offset.

        @return: list of provenance dictionaries
        TODO : are we sure that is a list of provenances?
        """
        params = dict(limit=limit)
        qs = urllib.urlencode(params)
        if selector_list is None:
            selector_list = [( "id", Op.GT, 0 )]
        if last_id is not None:
            selector_list += [("id", Op.GT, last_id)]
        selector_list += [("edge_type", Op.NOT_EQUAL, "None")]
        query = build_selector(selector_list)
        return self._request("GET", "/catalog/id=%s/dataset/id=%s/provenance/%s?%s"
                                    % (urlquote(catalog_id),
                                        urlquote(dataset_id),
                                        query, qs))

if __name__ == "__main__":
    # For testing with ipython
    import sys
    if len(sys.argv) < 3:
        print "Usage: %s goauth_token [base_url]" % sys.argv[0]
    #    sys.exit(1)
    goauth_token = "un=tanum|tokenid=728fb046-49be-11e4-89f7-22000ab68755|expiry=1443740304|client_id=tanum|token_type=Bearer|SigningSubject=https://nexus.api.globusonline.org/goauth/keys/24e59702-45a4-11e4-89f7-22000ab68755|sig=49ccb4f2aff39f7f5d93272f403d5263235e9eb708e316a0445ba9857aba98d47e7f847871d936a93a715324ce1e1cb0fa992de698b194f6e14f3c1b8f4bee5731435eeab673c637f18fe526f00d630fb38c98a7e4ea58de1416caae5b6d79adc8d7a141c9e9e437098077212de3323cd4aef39d412119592756470bbfd45978"#sys.argv[1]
    if len(sys.argv) > 2:
        base_url = sys.argv[2]
    else:
        base_url = DEFAULT_BASE_URL
    client = DatasetClient(goauth_token, base_url)

    #r, data = client.get_catalogs()

    create_initial_data=False

    if create_initial_data:
        r, mycatalog = client.create_catalog(config=dict(name="aaa1",description="test2"))
        print mycatalog
        r, dataset = client.create_dataset(mycatalog["id"],dict(name="test3"))
        print dataset
        #r, members = client.create_member(mycatalog["id"],dataset["id"],{"data_type":"session"})
        r, members = client.create_member(mycatalog["id"],dataset["id"],dict(data_type="file", data_uri="file://some_demo_file"))
        print members

    r, catalogs = client.get_catalogs()
    print catalogs


    mycatalog = client.get_catalog_by_name("aaa1")
    print mycatalog

    r, dataset = client.get_datasets(mycatalog["id"])
    print dataset
    newlist = sorted(dataset, key=lambda k: k['name'])
    print
    count = 0
    result = []
    last_dataset = {'name':'some_strange_string'}
    for i in newlist:
        if last_dataset['name'] != i['name']:
            result.append(i)
            last_dataset = i
            count +=1
        if count>4: # n-1
            break

    for i in newlist:
            print i
    print
    for i in result:
            print i
    print
    #print newlist
    exit(1)

    r, members = client.get_members(mycatalog["id"],dataset[0]["id"])
    print members

    r, members = client.create_member(mycatalog["id"],dataset[0]["id"],
                dict(data_type="file", data_uri="file://some_demo_file2%d"%len(members)))
    print members

    r, members2 = client.delete_member(mycatalog["id"],dataset[0]["id"],members["id"])
    print members2

    r, changes = client.get_changes(mycatalog["id"],dataset[0]["id"])
    for change in sorted(changes, key=lambda k: k['id']):
        print change
#    r, members = client.get_members(mycatalog["id"],dataset[0]["id"])
#    print members

#    print mycatalog["id"],dataset[0]["id"],members[0]["id"],{"change_type":"create",
#                                               "time_of_change":dataset[0]["created"],
#                                               "change_spec":"some change_spec" }

#    r, data = client.create_provenance(mycatalog["id"],dataset[0]["id"],{"edge_type":"wasgeneratedby",
#                                               "data_uri":"file://example",
#                                               "edge_ref":"22,23,24",
#                                               "transformation_ref": "10,11,12",
#                                               "dataset_reference_2":"44"})
#
#    print data

#    r, prov = client.get_provenances(mycatalog["id"],dataset[0]["id"])
#    print prov

#
#    r, data = client.create_change(mycatalog["id"],dataset[0]["id"],members[0]["id"],{"change_type":"create",
#                                               "time_of_change":dataset[0]["created"],
#                                               "change_spec":"some change_spec" })
#    r, data = client.create_edge(mycatalog["id"],dataset[0]["id"],members[0]["id"],{"data_type":"wasgenerated",
#                                               "data_uri":"file://example",
#                                               "dataset_edge":"22" })
#
#    print data


#    r, changes = client.get_edges(mycatalog["id"],dataset[0]["id"],members[0]["id"])
#    print changes
"""
#    r, data = client.create_dataset(6,dict(name="test5"))
#    print data
#
#    matches = re.search(r"%s" % match, catalogs)
#        if matches is not None:
#            self._progress_value = (int(matches.group(2)))
    """

#    mycatalog = client.get_catalog_by_name("aaa1")
#    print mycatalog
#
#    r, dataset = client.get_datasets(mycatalog["id"])
#    print dataset
#
#    mydataset = dataset[0] #mycatalog["id"],mydataset["id"]
#
#    r, data = client.create_member(6,50,{"data_type":"file", "data_uri":"myfile.txt"})
#    print data
#
#
"""
    #r, data = client.create_member(1,53,{"data_type":"session", "data_uri":"myfile.txt"})

    r, data = client.create_member(1,53,{"data_type":"session"})
    print data
    r, members = client.get_members(1,53)
    print members

    cmds = ["fist cmd", "second cmd"]
    input = ["one file", "one tmp file", "3rd file"]

    r, data = client.create_transformation(1,52,{"name":"3_rotate", "commands":"some commands", "input":"first second file", "output": "three"})
    print data


    r, transformations = client.get_transformations(1,52)
    print transformations
"""
#    r, transformations = client.delete_transformation(1,52,53)
#    print transformations

#    r, members = client.get_members(1,52)
#    print members