from scidataspace.client.commands.util import UNDEFINED,is_geounit_selected, run_command
from scidataspace.client.commands._leveldb2json import create_graph

import docker
import json
import os, sys
import re
import shutil

import hashlib
from datetime import datetime

def create_hash(input_string):
    h = hashlib.new('ripemd160')
    #input_string = "geounitname.geounitid.programname <with special chars stripped off>"
    str_now=str(datetime.now())
    h.update(input_string.encode('utf-8')+str_now)
    return h.hexdigest()

def build(cde_package_root, tag=None, cmd=None):
    # create Dockerfile
    with open(cde_package_root + '/Dockerfile', 'w') as f:
        f.write('''FROM ubuntu
COPY cde-root /
''')
        if cmd:
            f.write('CMD {0}\n'.format(cmd))

    # build image
    c = docker.Client(base_url='unix://var/run/docker.sock', version="1.12")
    docker_image_id=None
    for response in c.build(path=cde_package_root, tag=tag, rm=True):
        #print response,
        s = json.loads(response)
        if 'stream' in s:
            #print s['stream'],
            match = re.search('Successfully built (.*)', s['stream'])
            if match:
                docker_image_id = match.group(1)
        elif 'errorDetail' in s:
            raise Exception(s['errorDetail']['message'])

    if docker_image_id:
        print "Successfully built image id ",docker_image_id
    else:
        print "Could not create image"
    return docker_image_id

#######################################
#   Parse package
#######################################
def parse_cmd_package(cmd_splitted, catalog_id, geounit_id, datasetClient, db):
    if  not is_geounit_selected(geounit_id): return

    working_path = os.getcwd()
    current_path = os.path.dirname(os.path.abspath(__file__))
    #print "current_path=", current_path
    executable = os.path.join(current_path, "bin","ptu")
    packages_json_file = os.path.join(working_path, ".gdclient","packages","packages.json")
    try:
        with open(packages_json_file) as data_file:
            packages_json = json.load(data_file)
    except:
        packages_json = {}
    boolWithProvenance = False

    cmd_2 = cmd_splitted.get(1,"")
    ########
    #       list subcommand
    ########
    if cmd_2 == "list":
        print len(packages_json)," packages available:"
        for k in packages_json:
            print k,"  ",\
            packages_json[k]['date'],'   ',\
            packages_json[k]['command']
        return

    ########
    #       delete subcommand
    ########
    if cmd_2 == "delete":
        package_id = cmd_splitted.get(2,"")
        if packages_json.get(package_id,UNDEFINED) == UNDEFINED:
            print "cannot find package id ",package_id
            return
        packages_json.pop(package_id, None)
        package_directory = os.path.join(working_path, ".gdclient","packages",package_id)
        try:
            shutil.rmtree(package_directory)
        except Exception as e:
            print "cannot delet folder"
            sys.stderr.write(str(e) + "\n")

        with open(packages_json_file, 'w') as outfile:
            json.dump(packages_json, outfile, sort_keys = True, indent = 4)
        return


    cmd_level_index = 2
    # with provenance - create json
    if cmd_2 == "provenance":
        boolWithProvenance = True
        cmd_level_index += 1

    ########
    #       individual subcommand
    ########

    cmd_level = cmd_splitted.get(cmd_level_index,"")
    if cmd_level == 'individual':
        try:
            cde_directory = os.path.join(working_path, "cde-package")

            # make sure that LevelDB database does not exist
            if os.path.isdir(cde_directory):
                shutil.rmtree(cde_directory)

            user_command = ' '.join(cmd_splitted[cmd_level_index+1:])
            cmd_to_run = "%s %s 2>/dev/null" %(executable, user_command)
            # print "cmd_to_run=", cmd_to_run

            # this will create cde.option file and cde-package directory
            run_command(cmd_to_run)
            package_hash = create_hash(cmd_to_run)
            package_directory = os.path.join(working_path, ".gdclient","packages",package_hash)
            if not os.path.exists(package_directory):
                os.makedirs(package_directory)

            shutil.move(os.path.join(working_path, "cde.options"), package_directory)
            shutil.move(cde_directory, package_directory)

            # create json file, if is specified in command
            if boolWithProvenance:
                graph_dict = create_graph(package_directory+"cde-package/provenance.cde-root.1.log")

                json_file_name = os.path.join(package_directory,"filex.json")

                with open(json_file_name, 'w') as outfile:
                    json.dump(graph_dict, outfile, sort_keys = True, indent = 4)

            # need to store package hash in a list
            print "package_hash=",package_hash
            packages_json[package_hash]= dict(command= user_command, date=str(datetime.now()))
            with open(packages_json_file, 'w') as outfile:
                json.dump(packages_json, outfile, sort_keys = True, indent = 4)
        except:
            # print "Unexpected error:", sys.exc_info()[0]
            print "USAGE: --package level individual <program to execute>"
            pass
    ########
    #       collaboration subcommand
    ########

    elif cmd_level == 'collaboration':
        package_id = cmd_splitted.get(cmd_level_index+1,"")
        # test if individual is completed ; package exists
        package_directory = os.path.join(working_path, ".gdclient","packages",package_id)
        if packages_json.get(package_id,UNDEFINED) == UNDEFINED:
            print "cannot find package id ",package_id
            return
        package_directory = os.path.join(working_path, ".gdclient","packages",package_id)
        if not os.path.isdir(package_directory):
            print "ERROR: Package folder does not exists"
            return

        #  create a docker container
        docker_container_id = UNDEFINED
        try:
            # build('../cde-package', tag='scidataspace/test:v2', cmd='/root/d/hello.py')
            docker_container_id = build(package_directory+'/cde-package', tag=package_id)
            print "Successfull"
            return docker_container_id
        except Exception, ex:
            print "Error: {0}".format(ex)

    ########
    #       community subcommand
    ########

    elif cmd_level == 'community':
        # TODO: test if collaboration is completed ; docker file is created

        # TODO: put a docker file as part of docker container
        pass
    else:
        print "USAGE: package [provenance] list| delete| level [individual <program name>| collaboration <package id>| community] "
