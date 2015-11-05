scids-client is a python library for accessing interacting with SciDataspace. 

Pre-requirements
* git   
* python dev 
* sudo apt-get install python-setuptools
* g++
* docker

git clone https://<username>@bitbucket.org/tanum/scids-client.git

Install

Use the setup.py script to install this library:

cd scids-client/scidataspace/client
sudo python setup.py install
cd ../..
sudo python setup.py install

The library can also be installed as a normal user in a virtualenv, or using the --user option to install.

Usage

The client requires a goauth token to authenticate.