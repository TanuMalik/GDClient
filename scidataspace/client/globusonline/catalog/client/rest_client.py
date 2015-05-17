"""
Base client for JSON REST APIs using GoAuth.
"""
import httplib
import urllib
import json
import ssl
import socket
import time
from urlparse import urlparse
from collections import namedtuple
import logging

RETRY_WAIT_SECONDS = 5


RestResult = namedtuple("RestResult", "response body")


class GoauthRestClient(object):
    """
    Base client for Globusonline APIs which use Goauth and JSON. Specific
    APIs should define subclasses with helper methods.
    """
    def __init__(self, goauth_token, base_url, max_attempts=1, parse_json=True,
                 log_requests=False):
        self.goauth_token = goauth_token
        self.base_url = base_url
        self.max_attempts = max_attempts
        self.parse_json = parse_json

        self.log_requests = log_requests
        self._log = logging.getLogger("globusonline.catalog.rest_client")

        parsed_url = urlparse(base_url)
        self._is_https = (parsed_url.scheme == "https")

        netloc_parts = parsed_url.netloc.split(":")
        self._host = netloc_parts[0]
        if len(netloc_parts) == 1:
            if self._is_https:
                self._port = 443
            else:
                self._port = 80
        elif len(netloc_parts) == 2:
            self._port = int(netloc_parts[1])
        else:
            raise ValueError("Illegal netloc: '%s'" % parsed_url.netloc)
        self._base_path = parsed_url.path.rstrip("/")

        self._conn = None

    def _connect(self):
        if self._is_https:
            self._conn = httplib.HTTPSConnection(self._host, self._port)
        else:
            self._conn = httplib.HTTPConnection(self._host, self._port)

    def close(self):
        if self._conn:
            self._conn.close()
        self._conn = None

    def _request(self, method, path, body=None, expected_status=None):
        assert path.startswith("/")
        path = self._base_path + path
        headers = { "Authorization": "Globus-Goauthtoken %s"
                                     % self.goauth_token,
                    "Accept": "application/json" }
        if body:
            headers["Content-Type"] = "application/json"

        if self.log_requests:
            self._log.info("%s %s", method, path)

        def do_request():
            if self._conn is None:
                self._connect()
            self._conn.request(method, path, body=body, headers=headers)
            r = self._conn.getresponse()
            response_body = r.read()
            return r, response_body

        for attempt in xrange(self.max_attempts):
            r = None
            try:
                try:
                    r, response_body = do_request()
                except httplib.BadStatusLine:
                    # This happens when the connection is closed by the server
                    # in between request, which is very likely when using
                    # interactively, in a client that waits for user input
                    # between requests, or after a retry wait. This does not
                    # count as an attempt - it just means the old connection
                    # has gone stale and we need a new one.
                    # TODO: find a more elegant way to re-use the connection
                    #       on closely spaced requests. Can we tell that the
                    #       connection is dead without making a request?
                    self.close()
                    r, response_body = do_request()
            except ssl.SSLError:
                # This probably has to do with failed authentication, so
                # retrying is not useful.
                self.close()
                raise
            except socket.error:
                # Network error. If the last attempt failed, raise,
                # otherwise do nothing and go on to next attempt.
                self.close()
                if attempt == self.max_attempts - 1:
                    raise

            # Check for 503 ServiceUnavailable, which is treated just like
            # network errors.
            if (r is not None and r.status == 503
            and attempt < self.max_attempts - 1):
                # Force sleep below and continue loop, unless we are on
                # the last attempt in which case skip this and return
                # the 503 error.
                self.close()
                r = None

            if r is not None:
                break
            else:
                time.sleep(RETRY_WAIT_SECONDS)

        content_type = r.getheader("Content-Type")
        if (content_type and "application/json" in content_type
        and self.parse_json and response_body):
            response_body = json.loads(response_body)

        error = False
        if expected_status is None:
            if r.status >= 400 or r.status < 200:
                error = True
        elif r.status != expected_status:
            error = True
        if error:
            raise RestClientError(r, response_body, expected_status)
        return RestResult(r, response_body)


class RestClientError(Exception):
    """Generic rest error exception, which encapsulates the httplib response
    and body, and provides convenient attribute access to error fields if the
    error body is a dictionary (typically loaded from a JSON string in higher
    level code).
    """
    def __init__(self, response, body, expected_status=None):
        self.response = response
        self.body = body
        self.expected_status = expected_status
        message = ["RestClientError httpstatus='%d %s'" % (
                        response.status, response.reason)]
        if expected_status:
            message[0] += " (expected status %d)" % expected_status
        if body:
            if isinstance(body, dict):
                message += ("%s='%s'" % item for item in body.iteritems())
            else:
                message += ("body='%s'" % body[:1000],)
        Exception.__init__(self, ", ".join(message))

    def __getattr__(self, name):
        if self.body and isinstance(self.body, dict):
            value = self.body.get(name, _NOT_SET)
            if value is not _NOT_SET:
                return value
        raise AttributeError()


_NOT_SET = object()


def urlquote(x):
    """Quote a str, unicode, or value coercable to str, for safe insertion
    in a URL.

    Uses utf8 encoding for unicode. Also note that the tagfiler
    delimiters comma, semicolon, and equals sign will be percent
    encoded, so this is effective to use quoting individual query
    elements but should never be used on the full query.

    >>> urlquote("n;a,m e=")
    'n%3Ba%2Cm%20e%3D'
    >>> urlquote(100)
    '100'
    >>> urlquote(True)
    'True'
    >>> urlquote(False)
    'False'

    Unicode, small a with macron:
    >>> urlquote(u'\u0101')
    '%C4%81'

    utf8 already encoded:
    >>> urlquote("\xc4\x81")
    '%C4%81'
    """
    return urllib.quote(safestr(x), "")

def urlunquote(x):
    return urllib.unquote(x)

def safestr(x):
    """Convert x to str, encoding in utf8 if needed.

    Can be used to pre-process data passed to urlencode."""
    assert x is not None
    if isinstance(x, unicode):
        return x.encode("utf8")
    elif not isinstance(x, str):
        return str(x)
    return x


if __name__ == "__main__":
    # For testing with ipython
    import sys
    if len(sys.argv) < 4:
        print "Usage: %s goauth_token base_url" % sys.argv[0]
        sys.exit(1)
    goauth_token = sys.argv[2]
    base_url = sys.argv[3]
    client = GoauthRestClient(goauth_token, base_url)
