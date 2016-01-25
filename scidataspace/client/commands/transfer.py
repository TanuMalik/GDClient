from scidataspace.client.commands.util import UNDEFINED,run_command

import re
import os
import urllib
from sys import platform as _platform

#######################################
#   Parse transfer
#######################################
# def parse_cmd_transfer(cmd_splitted, image_id=None, cfg=None):
#     # This version is with Docker image ; we'll need later, but now we don't use Docker
#     image_id = cmd_splitted.get(1,UNDEFINED)
#     destination = cmd_splitted.get(2,"undefined")
#     output = os.system("docker images | grep "+image_id+" | wc -l")
#     if str(output).strip()=="1":
#         print "Cannot find image "+image_id+" for transfer; please run --package level collaboration"
#         return
#
#     if not cfg.config['GLOBUS']['local-endpoint']:
#         print "Cannot obtain from config file: GLOBUS endpoint"
#         return
#     if not cfg.config['GLOBUS']['local-folder']:
#         print "Cannot obtain from config file: local folder shared in GLOBUS "
#         return
#     if not cfg.config['GLOBUS']['globus-folder']:
#         print "Cannot obtain from config file: globus folder shared in local GLOBUS endpoint"
#         return
#
#     print "Please wait. Saving image ..."
#     image_file = cfg.config['GLOBUS']['local-folder']+"/docker"+image_id.strip()+".tar "
#
#     command = "docker save --output="+image_file + image_id
#     # print "will run: ",command
#     run_command(command)
#     source_endpoint  = cfg.config['GLOBUS']['local-endpoint'] # 'globuspublish#cirlab'
#     source_folder= cfg.config['GLOBUS']['globus-folder'] # '/globuspublication_52/'
#
#     # command = "ssh cvlaescx@cli.globusonline.org scp cvlaescx#test_2:/docker_image/dockeraf8db02049ae25d3e64436769411206258b3b003.tar cvlaescx#test:/home/cristian/Installs/new3.tar"
#     source = "%s:%s%s" %(source_endpoint,source_folder, "docker"+image_id.strip()+".tar " )
#     command = "ssh %s@cli.globusonline.org scp -D %s %s" % (cfg.config['Default']['uname'], source, destination)
#
#     # print "will run: ",command
#     run_command(command)
#     #print "USAGE: transfer [destination_endpoint:destination_folder]"

def parse_cmd_transfer(cmd_splitted, image_id=None, cfg=None):
    image_id = cmd_splitted.get(1,UNDEFINED)
    packages_directory = os.path.join(os.path.expanduser("~"), ".gdclient","packages")
    package_directory = os.path.join(packages_directory, image_id)

    if not os.path.isdir(package_directory):
        print "Cannot find image "+image_id+" for transfer; please run --package level collaboration"
        return

    if not cfg.config['GLOBUS']['local-folder']:
        print "Cannot obtain from config file: local folder shared in GLOBUS "
        return
    if not cfg.config['GLOBUS']['local-endpoint']:
        print "Cannot obtain from config file: GLOBUS local endpoint"
        return
    if not cfg.config['GLOBUS']['remote-endpoint']:
        print "Cannot obtain from config file: GLOBUS remote endpoint"
        return
    if not cfg.config['GLOBUS']['globus-remote-folder']:
        print "Cannot obtain from config file: globus folder shared in remote GLOBUS endpoint"
        return
    if not cfg.config['GLOBUS']['globus-local-folder']:
        print "Cannot obtain from config file: globus folder shared in local GLOBUS endpoint"
        return

    print "Please wait. Saving image ..."
    short_image_name = image_id.strip()+".tgz "
    image_file = cfg.config['GLOBUS']['local-folder']+"/"+short_image_name

    command = "cd "+packages_directory+";tar czf "+image_file+" "+ image_id
    # print "will run: ",command
    run_command(command)
    source_endpoint  = cfg.config['GLOBUS']['local-endpoint'] # 'globuspublish#cirlab'
    source_folder= cfg.config['GLOBUS']['globus-local-folder'] # '/globuspublication_52/'
    remote_endpoint  = cfg.config['GLOBUS']['remote-endpoint'] # 'globuspublish#cirlab'
    remote_folder= cfg.config['GLOBUS']['globus-remote-folder'] # '/globuspublication_52/'

    # command = "ssh cvlaescx@cli.globusonline.org scp cvlaescx#test_2:/docker_image/dockeraf8db02049ae25d3e64436769411206258b3b003.tar cvlaescx#test:/home/cristian/Installs/new3.tar"
    source = "%s%s%s" %(source_endpoint,source_folder, short_image_name)
    destination = "%s%s%s" %(remote_endpoint,remote_folder, short_image_name)
    command = "ssh %s@cli.globusonline.org transfer -- %s %s" % (cfg.config['Default']['uname'], source, destination)
    # print "will run: ",command
    run_command(command)
    #print "USAGE: transfer [destination_endpoint:destination_folder]"
