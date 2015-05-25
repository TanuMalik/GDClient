from util import UNDEFINED,run_command

import re
import os
import urllib
from sys import platform as _platform

#######################################
#   Parse transfer
#######################################
def parse_cmd_transfer(cmd_splitted, image_id=None, cfg=None):
    image_id = cmd_splitted.get(1,UNDEFINED)
    output = os.system("docker images | grep "+image_id+" | wc -l")
    if str(output).strip()=="1":
        print "Cannot find image "+image_id+" for transfer; please run --package level collaboration"
        return

    if not cfg.config['GLOBUS']['local-endpoint']:
        print "Cannot obtain from config file: GLOBUS endpoint"
        return
    if not cfg.config['GLOBUS']['local-folder']:
        print "Cannot obtain from config file: local folder shared in GLOBUS "
        return
    if not cfg.config['GLOBUS']['globus-folder']:
        print "Cannot obtain from config file: globus folder shared in local GLOBUS endpoint"
        return

    print "Please wait. Saving image ..."
    image_file = cfg.config['GLOBUS']['local-folder']+"/docker"+image_id.strip()+".tar "
    run_command("docker save "
                "--output="+image_file
                + image_id)
    start_url='https://www.globus.org/xfer/StartTransfer#origin='
    source_endpoint  = cfg.config['GLOBUS']['local-endpoint'] # 'globuspublish#cirlab'
    source_folder= cfg.config['GLOBUS']['globus-folder'] # '/globuspublication_52/'

    URI =  urllib.quote_plus(source_endpoint+source_folder)

    #print start_url+URI

    command = "echo "
    if _platform == "linux" or _platform == "linux2":
        # linux
        command = "xdg-open "
        pass
    elif _platform == "darwin":
        # MAC OS X
        command = "open "
        pass
    elif _platform == "win32":
        # Windows, we don't support this, yet
        pass

    run_command(command+start_url+URI+" >/dev/null 2>/dev/null &")
    #print "USAGE: transfer [destination_endpoint:destination_folder]"
