import os
import json
from os.path import expanduser
home_directory = expanduser("~")
from scidataspace.client.commands.util import run_command
from scidataspace.client.commands._leveldb2json import create_graph

#######################################
#   Parse track
#######################################
def parse_cmd_track(cmd_splitted):

    cmd_2 = ' '.join(cmd_splitted[1:])

    working_path = os.getcwd()
    # make sure that LevelDB database does not exist
    run_command("rm -rf "+os.path.join(working_path, "cde-package"))

    current_path = os.path.dirname(os.path.abspath(__file__))
    ptu_path = os.path.join(current_path, "bin","ptu")

    cmd_to_run = "%s -b %s" %(ptu_path, cmd_2)
    #print "cmd_to_run=", cmd_to_run

    # this will create cde.option file and cde-package directory
    run_command(cmd_to_run)

    graph_dict = create_graph("cde-package/provenance.cde-root.1.log")

    run_command(" rm -rf cde-package cde.options")
    json_file_name = os.path.join(home_directory,".gdclient","filex.json")

    with open(json_file_name, 'w+') as outfile:
        json.dump(graph_dict, outfile, sort_keys = True, indent = 4)

    #print "USAGE: track <program name with arguments>"
