import  os
import re
from scidataspace.client.commands.util import is_geounit_selected

###
##  this function needs debug
###
def cmd_count_files(process):
    cnt_files = 0
    for line in iter(process.stdout.readline, ''):
        matches = re.search("(.*) <(\d*)> .*", line.strip())
        if matches is not None:
            member_name = matches.group(1)
            member_id = matches.group(2)
            cnt_files += 1
            #db.Put("member."+member_name, member_id)

            print "   > ", member_name," with id ", member_id
    return cnt_files

#######################################
#   Parse add_member
#######################################
def parse_cmd_add_member(cmd_splitted,catalog_id, geounit_id, datasetClient, db):
    #global db
    if  not is_geounit_selected(geounit_id): return

    cmd_2 = cmd_splitted.get(1,"")

    if cmd_2 == "":
        print "USAGE: add_member [<file_name> | <folder name>]"

    # add file as member
    elif os.path.isfile(cmd_2):
        try:
            r, members = datasetClient.create_member(catalog_id,geounit_id,dict(data_type="file", data_uri=cmd_2))
            print members['id']
            db.Put("member."+cmd_2, str(members['id']))
        except:
            print "cannot add member "+cmd_2
            pass

    # add members from folder
    elif os.path.isdir(cmd_2):

        members_list = [dict(data_type="directory", data_uri=os.path.join(cmd_2))]
        for dirname, dirnames, filenames in os.walk(cmd_2):
            for subdirname in dirnames:
                members_list.append(dict(data_type="directory", data_uri=os.path.join(dirname, subdirname)))
            for filename in filenames:
                members_list.append(dict(data_type="file", data_uri=os.path.join(dirname, filename)))

        print "adding:",str(members_list)
        _, members = datasetClient.create_members(catalog_id,geounit_id,members_list)
        print members
        #print members.get('code','Error')

    # add_member something
    else:
        print "cannot find file or folder with name "+cmd_2
