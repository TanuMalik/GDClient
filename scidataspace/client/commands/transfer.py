from scidataspace.client.commands.util import UNDEFINED,run_command

import re
import os
import urllib
from sys import platform as _platform
from globusonline.transfer.api_client import Transfer, create_client_from_args
from datetime import datetime, timedelta
import transfer_to_hydroshare

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

def activate_endpoint(api, endpoint_name):
    code, reason, result = api.endpoint_autoactivate(endpoint_name,
                                                     if_expires_in=600)
    if result["code"].startswith("AutoActivationFailed"):
        print "Auto activation failed, ls and transfers will likely fail!"
    # print "result: %s (%s)" % (result["code"], result["message"])


def globus_transfer(image_id, cfg):
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
    short_image_name = image_id.strip()+".tgz"
    image_file = cfg.config['GLOBUS']['local-folder']+"/"+short_image_name

    command = "cd "+packages_directory+";tar czf "+image_file+" "+ image_id
    # print "will run: ",command
    run_command(command)
    source_endpoint  = cfg.config['GLOBUS']['local-endpoint'] # 'globuspublish#cirlab'
    source_folder= cfg.config['GLOBUS']['globus-local-folder'] # '/globuspublication_52/'
    remote_endpoint  = cfg.config['GLOBUS']['remote-endpoint'] # 'globuspublish#cirlab'
    remote_folder= cfg.config['GLOBUS']['globus-remote-folder'] # '/globuspublication_52/'

    # # command = "ssh cvlaescx@cli.globusonline.org scp cvlaescx#test_2:/docker_image/dockeraf8db02049ae25d3e64436769411206258b3b003.tar cvlaescx#test:/home/cristian/Installs/new3.tar"
    # source = "%s%s%s" %(source_endpoint,source_folder, short_image_name)
    # destination = "%s%s%s" %(remote_endpoint,remote_folder, short_image_name)
    # command = "ssh %s@cli.globusonline.org transfer -- %s %s" % (cfg.config['Default']['uname'], source, destination)
    # # print "will run: ",command
    # run_command(command)
    # #print "USAGE: transfer [destination_endpoint:destination_folder]"

    transfer_args = [cfg.config['Default']['uname'], '-g', cfg.config['Default']['goauth-token']]

    TransferAPIClient, _ = create_client_from_args(transfer_args)

    activate_endpoint(TransferAPIClient, source_endpoint)
    activate_endpoint(TransferAPIClient, remote_endpoint)

    # submit the transfer
    code, message, data = TransferAPIClient.transfer_submission_id()
    submission_id = data["value"]
    deadline = datetime.utcnow() + timedelta(minutes=10)
    t = Transfer(submission_id, source_endpoint, remote_endpoint, deadline)
    source_image_name = "%s%s"%(source_folder,short_image_name)
    destination_image_name = "%s%s"%(remote_folder,short_image_name)
    t.add_item(source_image_name, destination_image_name)
    code, reason, data = TransferAPIClient.transfer(t)
    task_id = data["task_id"]
    print "Submitted transfer id: ", task_id
    return "globus://%s%s%s"%(remote_endpoint,remote_folder,short_image_name)


def parse_cmd_transfer(cmd_splitted, image_id=None, cfg=None):
    transfer_to_hydroshare.transfer_to_hydroshare_server()
  
    #image_id = cmd_splitted.get(1,UNDEFINED)
    #globus_transfer(image_id, cfg)
