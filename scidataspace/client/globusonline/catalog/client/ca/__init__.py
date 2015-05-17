CA_FILE = "all-ca.pem"

def get_ca(base_url_or_hostname):
    # Note: use the same CA file for all servers. This is simpler and
    # facilitates the transition from GoDaddy to InCommon for the api servers.
    try:
        import pkg_resources
        path = pkg_resources.resource_filename(__name__, CA_FILE)
    except ImportError:
        pkg_path = os.path.dirname(__file__)
        path = os.path.join(pkg_path, CA_FILE)
    return path
