from util import UNDEFINED
from util import is_geounit_selected, run_command

import docker
import json
import os
import re

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
    docker_image_id=''
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
        print "Successfully built image id ",docker_image_id.strip()
    else:
        print "Could not create image"

#######################################
#   Parse package
#######################################
def parse_cmd_package(cmd_splitted, catalog_id, geounit_id, datasetClient, db):
    if  not is_geounit_selected(geounit_id): return

    cmd_2 = cmd_splitted.get(1,"")

    executable = "ls" # cde
    cmd_level_index = 2
    # annotate geounit
    if cmd_2 == "provenance":
        executable = "ls -l"  # ptu
        cmd_level_index = 3

    cmd_level = cmd_splitted.get(cmd_level_index,"")
    if cmd_level == 'individual':
        #output = run_command(executable+' '+' '.join(cmd_splitted[cmd_level_index+1:]))
        working_path = os.getcwd()
        print run_command("rm -rf "+os.path.join(working_path, "cde-package"))

        current_path = os.path.dirname(os.path.abspath(__file__))
        print "current_path=", current_path

        ptu_path = os.path.join(current_path, "provenance-to-use-tmp","ptu")
        cmd_2 = "ls q*"
        cmd_to_run = "%s %s" %(ptu_path, cmd_2)
        print "cmd_to_run=", cmd_to_run
        print run_command(cmd_to_run)
        print run_command("ls -l "+os.path.join(working_path, "cde-package"))
        # now we have "cde-package" in working_path


    elif cmd_level == 'collaboration':
        # test if individual is completed ; package exists
        working_path = os.getcwd()
        cde_path = os.path.join(working_path, "cde-package")
        if not os.path.isdir(cde_path):
            print "Package does not exists; please use option 'individual' "
            return

        #  create a docker container
        try:
            # build('../cde-package', tag='scidataspace/test:v2', cmd='/root/d/hello.py')
            build('cde-package', tag='scidataspace/test:v2')
        except Exception, ex:
            print "Error: {0}".format(ex)
    elif cmd_level == 'community':
        # TODO: test if collaboration is completed ; docker file is created

        # TODO: put a docker file as part of docker container
        pass
    else:
        print "USAGE: package [provenance] [individual |collaboration| community] package-directory"
