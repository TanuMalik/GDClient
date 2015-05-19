# 
# Copyright 2010 University of Southern California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Tagfiler client.
"""

from globusonline.catalog.client.models import File

from httplib import HTTPConnection, HTTPSConnection
from httplib import OK, CREATED, ACCEPTED, NO_CONTENT, SEE_OTHER
import urlparse
import urllib
import logging
import socket

try:
    import simplejson #@UnresolvedImport
    json = simplejson
except:
    import json as imported_json
    json = imported_json


logger = logging.getLogger(__name__)


class TagfilerException(Exception):
    def __init__(self, value, cause=None):
        super(TagfilerException, self).__init__(value)
        self.value = value
        self.cause = cause
        
    def __str__(self):
        message = "%s." % self.value
        if self.cause:
            message += " Caused by: %s." % self.cause
        return message

class MalformedURL(TagfilerException):
    """MalformedURL indicates a malformed URL.
    """
    def __init__(self, cause=None):
        super(MalformedURL, self).__init__("URL was malformed", cause)

class UnresolvedAddress(TagfilerException):
    """UnresolvedAddress indicates a failure to resolve the network address of
    the Tagfiler service.
    
    This error is raised when a low-level socket.gaierror is caught.
    """
    def __init__(self, cause=None):
        super(UnresolvedAddress, self).__init__("Could not resolve address of host", cause)

class NetworkError(TagfilerException):
    """NetworkError wraps a socket.error exception.
    
    This error is raised when a low-level socket.error is caught.
    """
    def __init__(self, cause=None):
        super(NetworkError, self).__init__("Network I/O failure", cause)

class ProtocolError(TagfilerException):
    """ProtocolError indicates a protocol-level failure.
    
    In other words, you may have tried to add a tag for which no tagdef exists.
    """
    def __init__(self, message='Network protocol failure', errorno=-1, response=None, cause=None):
        super(ProtocolError, self).__init__("Tagfiler protocol failure", cause)
        self._errorno = errorno
        self._response = response
        
    def __str__(self):
        message = "%s." % self.value
        if self._errorno >= 0:
            message += " HTTP ERROR %d: %s" % (self._errorno, self._response)
        return message
    
class NotFoundError(TagfilerException):
    """Raised for HTTP NOT_FOUND (i.e., ERROR 404) responses."""
    pass


class TagfilerClient(object):
    """Web service client used to interact with the Tagfiler REST service."""

    def __init__(self, url, username, password=None, globus_token=None, dataset_id=None, catalog_name=None):
        """Initializes the Tagfiler client object.
        """
        pieces = urlparse.urlparse(url) #TODO: does this throw exceptions?!
        
        self.scheme = pieces[0]
        host_port = pieces[1].split(":")
        self.host = host_port[0]
        self.port = None
        if len(host_port) > 1:
            self.port = host_port[1]
        self.baseuri = pieces[2]
        self.username = username
        self.password = password
	self.dataset_id = dataset_id
	self.catalog_name = catalog_name
	self.globus_token = globus_token
        self.connection_class = None
        if self.scheme == "https":
            self.connection_class = HTTPSConnection
        else:
            self.connection_class = HTTPConnection
        self.connection = None
        self.cookie = None
	
        
        if not self.host or not len(self.host):
            raise MalformedURL(cause='Hostname cannot be None')
        
        if self.port and len(self.port):
            try:
                self.port = int(self.port)
            except Exception:
                raise MalformedURL(cause='Invalid port number (%s)' % self.port)


    def connect(self):
        """Connects to the Tagfiler service."""
        assert not self.connection
        self.connection = self.connection_class(host=self.host, port=self.port)


    def login(self):
        """Login to the Tagfiler service.
        
        Raises 'UnresolvedAddress' if unable to resolve the hostname.
        
        Raises 'NotFoundError' if the Tagfiler login resource is not found.
        """
        assert self.connection
        headers = {}
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        try:
            loginurl = "%s/session" % self.baseuri
            resp = self._send_request("POST", loginurl, 
                                      "username=%s&password=%s" % \
                                      (self.username, self.password), headers)
            self.cookie = resp.getheader("set-cookie")
        except socket.gaierror as e:
            raise UnresolvedAddress(e)
        except ProtocolError as e:
            raise


    def close(self):
        """Closes the connection to the Tagfiler service.
        
        The underlying python documentation is not very helpful but it would
        appear that the HTTP[S]Connection.close() could raise a socket.error.
        Thus, this method potentially raises a 'NetworkError'.
        """
        assert self.connection
        try:
            self.connection.close()
        except socket.error as e:
            raise NetworkError(e)
        finally:
            self.connection = None
            self.cookie = None


    def _send_request(self, method, url, body='', headers={}):
        try:
            self.connection.request(method, url, body, headers)
            resp = self.connection.getresponse()
            if resp.status not in [OK, CREATED, ACCEPTED, NO_CONTENT, SEE_OTHER]:
                raise ProtocolError(errorno=resp.status, response=resp.read())
        except socket.error as e:
            raise NetworkError(e)
        return resp

    # Kyle added - used to grab all the tags from all the files and apply them to the specified dataset
    # this method will no longer create a file entry for each file object although we could extend it
    # if we need to
    def add_subjects(self, fileobjs):
        parsed_table = []
        tag_names = []
        
        tag_sets = []
        for fileobj in fileobjs:
            tag_sets.append(fileobj.tags)
            tag_sets.extend(fileobj.content_tags)
        
	parsed_dict = {}
	dataset_name = None

        for tag_set in tag_sets:
            for tag in tag_set:
                tag_list = parsed_dict.get(tag.name, [])
                tag_list.append(tag.value)
		if tag.name == 'dataset_name':
		    dataset_name = tag_list[0]
                elif tag.name != "name" :
                    parsed_dict[tag.name] = tag_list
                    if tag.name not in tag_names:
                        tag_names.append(tag.name)

	

	dataset_id = self.dataset_id
	if self.dataset_id == None and dataset_name == None:
		print "No dataset name has been specified"

	# check if this dataset already exists, if not create one
	if dataset_name != None:
		try:
			dataset = self.find_subject_id_by_datasetname(dataset_name)
		except:
			print "Dataset %s does not exist. Creating new dataset." % dataset_name
			create_url = '%s/subject/dataset_name=%s' % (self.baseuri, dataset_name)
			headers = {"Authorization": "Globus-Goauthtoken %s" % self.globus_token}
	      	 	self._send_request("POST", create_url, '', headers)
			dataset = self.find_subject_id_by_datasetname(dataset_name)
		dataset_id = dataset[0]['id']

	# add the catalog name
	set_tags = 'catalog=%s' %(self._safequote(self.catalog_name))
	for key in parsed_dict:
		set_tags = '%s;%s=%s' %(set_tags, key, ','.join(parsed_dict[key]))
	
	print set_tags
	# remove the first ;, and ensure all spaces are encoded
	# TODO we should encode a lot more 
	#set_tags = set_tags[1:]
	set_tags = set_tags.replace(" ", "%20")

	bulkurl = '%s/tags/id=%s(%s)' % (self.baseuri, dataset_id,set_tags)
	headers = {"Authorization": "Globus-Goauthtoken %s" % self.globus_token}
 	self._send_request("PUT", bulkurl, '', headers)

    def add_subjects_old(self, fileobjs):
        """Registers a list of files and tags in tagfiler using a single request.
        
        Keyword arguments:
        
        fileobjs -- the list of register files objects 
        
        """
        parsed_table = []
        tag_names = []
        
        tag_sets = []
        for fileobj in fileobjs:
            tag_sets.append(fileobj.tags)
            tag_sets.extend(fileobj.content_tags)
        
        for tag_set in tag_sets:
            # TODO: need to remove the following comment. 'name' used to be 
            #   required so we would catch it here and raise an error, but
            #   that is no longer the case.
            #
            # name is a required tag
            #if not len(fileobj.filter_tags("name")):
            #    raise ValueError("Register file %s must have its 'name' tag set." % unicode(fileobj))
            parsed_dict = {}
            for tag in tag_set:
                tag_list = parsed_dict.get(tag.name, [])
                tag_list.append(tag.value)
                parsed_dict[tag.name] = tag_list
                if tag.name != "name":
                    if tag.name not in tag_names:
                        tag_names.append(tag.name)
            parsed_table.append(parsed_dict)
        payload = json.dumps(parsed_table)
        bulkurl = '%s/subject/name(%s)' % (self.baseuri, ';'.join([ self._safequote(tag) for tag in tag_names ]))

	if self.globus_token:        
	    headers = {"Content-Type": "application/json", "Authorization": "Globus-Goauthtoken %s" % self.globus_token}
        else:
	    headers = {"Content-Type": "application/json", "Cookie": self.cookie}
	
 	self._send_request("PUT", bulkurl, payload, headers)

    # Kyle added to get datasets
    def find_subject_id_by_datasetname(self, dataset_name):
        """Looks up a subject by its name tag in tagfiler and returns a dictionary if found, None otherwise
        
        Keyword arguments:
        name -- name to query
        """
        url = "%s/tags/dataset_name=%s" % (self.baseuri, self._safequote(dataset_name))
        headers = {"Authorization": "Globus-Goauthtoken %s" % self.globus_token, "Accept": "application/json"}
        resp = self._send_request("GET", url, headers=headers)
        subject = json.loads(resp.read())
        return subject

    def find_subject_by_name(self, name):
        """Looks up a subject by its name tag in tagfiler and returns a dictionary if found, None otherwise
        
        Keyword arguments:
        name -- name to query
        """
        url = "%s/tags/name=%s" % (self.baseuri, self._safequote(name))
        headers = {"Cookie": self.cookie, "Accept": "application/json"}
        resp = self._send_request("GET", url, headers=headers)
        subject = json.loads(resp.read())
        return subject


    def _safequote(self, s):
        return urllib.quote(s, '')


    '''
    TODO: this commented code should be removed. I'm only keep it temporarily, 
    because I will not remember how to do this non-bulk call!
    
    def add_subject(self, fileobj):
        """Registers a single file in tagfiler
        
        Keyword arguments:
        fileobj -- models.File object with tags
        """
        assert isinstance(fileobj, File)

        # name is a required tag
        if not len(fileobj.filter_tags("name")):
            raise ValueError("Register file %s must have its 'name' tag set." % unicode(fileobj))

        # Remove the name tag from the file tags, since this is specified outside the query string
        tag_pairs = []
        for tag in fileobj.tags:
            if tag.name != "name":
                tag_pairs.append("%s=%s" % (self._safequote(tag.name), self._safequote(tag.value)))
        url = "%s/subject/name=%s?%s" % (self.baseuri, 
                    self._safequote(fileobj.filter_tags("name")[0].value), "&".join(tag_pairs))
        headers = {"Cookie": self.cookie}
        self._send_request("PUT", url, headers=headers)

    '''
