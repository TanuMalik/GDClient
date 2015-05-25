catalog-client
==============

catalog-client is a python library for accessing the Globus Online Catalog
REST API. Note that the API is currently in alpha.

Install
=======

Use the setup.py script to install this library:

::

    sudo python setup.py install

The library can also be installed as a normal user in a virtualenv, or using
the --user option to install.

Usage
=====


The catalog API requires a goauth token to authenticate. See examples in
globusonline/catalog/client/examples/\*. The examples can also be run directly:

::

   python -m 'globusonline.catalog.client.examples.list_catalogs' 
