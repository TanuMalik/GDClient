from util import UNDEFINED
from util import is_geounit_selected, run_command

import docker
import json

def build(cde_package_root, tag=None, cmd=None):
    # create Dockerfile
    with open(cde_package_root + '/Dockerfile', 'w') as f:
        f.write('''FROM ubuntu
COPY cde-root /
''')
        if cmd:
            f.write('CMD {0}\n'.format(cmd))

    # build image
    c = docker.Client()
    for response in c.build(path=cde_package_root, tag=tag, rm=True):
        #print response,
        s = json.loads(response)
        if 'stream' in s:
            print s['stream'],
        elif 'errorDetail' in s:
            raise Exception(s['errorDetail']['message'])



#######################################
#   Parse package
#######################################
def parse_cmd_package(cmd_splitted, catalog_id, geounit_id, datasetClient):
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
        output = run_command(executable+' '+' '.join(cmd_splitted[cmd_level_index+1:]))
        print output
    elif cmd_level == 'collaboration':
        # TODO: test if individual is completed ; package exists

        #  create a docker container
        try:
            build('cde-package', tag='scidataspace/test:v2', cmd='/root/d/hello.py')
        except Exception, ex:
            print "Error: {0}".format(ex)
    elif cmd_level == 'community':
        # TODO: test if collaboration is completed ; docker file is created

        # TODO: put a docker file as part of docker container
        pass
    else:
        print "USAGE: package [provenance] [individual |collaboration| community] package-directory"
