from leveldb import LevelDB, LevelDBError
import os, sys
import logging
import string
import datetime,json, re
from pprint import pprint

import argparse
from os.path import expanduser
home_directory = expanduser("~")
default_output_file = os.path.join(home_directory,".gdclient","filex.json")
default_input_file = "cde-package/provenance.cde-root.1.log"

version = "1.0"
geounit_namespace = "prov"

def print_my_leveldb(db, selections):
    printset = set(string.printable)
    totalrange = 20
    #selections=['pidnone']#,'prv.iopid','file','prv.iofile','prv.file']
    for selection in selections:
        for (k, v) in db.RangeIter(key_from=selection, key_to=selection+'zzz'):

            if set(v).issubset(printset):
                print "[%s] '%s' -> [%s] '%s'" % (len(k), k, len(v), v)
            else:
                hexv = v.encode('hex')
                strv = ''.join([x if (ord(x) > 31 and ord(x)<128) else ('\\'+str(ord(x))) for x in v])
                if len(hexv) <= totalrange:
                    print "[%s]x '%s' -> [%s] '0x%s' %s" % (len(k), k, len(v), hexv, strv)
                else:
                    hexv = hexv[:totalrange/2] + '...' + hexv[-totalrange/2:]
                    print "[%s]xx '%s' -> [%s] '0x%s' %s" % (len(k), k, len(v), hexv, strv)


def isFilteredPath(path):
  if re.match('\/proc\/', path) is None \
    and re.match('.*\/lib\/', path) is None \
    and re.match('\/etc\/', path) is None \
    and re.match('\/var\/', path) is None \
    and re.match('\/dev\/', path) is None \
    and re.match('\/sys\/', path) is None \
    and re.match('.*\/R\/x86_64-pc-linux-gnu-library\/', path) is None \
    and re.match('.*\/usr\/share\/', path) is None:
    return True
  else:
    return False

def open_db(logfile):
    dberror = ''
    db = None
    if os.path.exists(logfile+'_db'):
        try:
            db = LevelDB(logfile+'_db', create_if_missing = False)
        except LevelDBError:
            dberror = 'LevelDBError'

        if dberror != '':
            logging.error(dberror)
            sys.exit(-1)
    return db

# activity == process
# entity == file

def find_activities(db):
    activities_dict = {} #pidkey : activity_name
    processes=[v for (k, v) in db.RangeIter(key_from='pid.', key_to='pid.z')]
    for process in processes:
        process_path = [v for (k, v) in db.RangeIter(key_from='prv.pid.'+process+'.path', key_to='prv.pid.'+process+'.pathz')]
        if len(process_path) != 0:
            activities_dict[process]=process_path[0]
    return activities_dict


def create_graph(input_file):

    db=open_db(input_file)
    if not db:
        print "Cannot obtain link to LevelDB database"
        return None
    # print_my_leveldb(db, ['pid','prv.iopid','file','prv.iofile','prv.file'])

    def format_time(unix_time):
        starttime=float(unix_time)/1000000
        return str(datetime.datetime.utcfromtimestamp(starttime).isoformat())

    mydict = {}

    mydict['prefix'] = {geounit_namespace: "http://example.org"}

    # populate activities
    activities_dict = find_activities(db)
    counter_entity=1
    for pidkey in activities_dict:
        activities = mydict.get('activity',{})

        #activity_name = geounit_namespace+":"+activities_dict[pidkey]
        activity_name = geounit_namespace+":act"+str(counter_entity)+"--"+activities_dict[pidkey]
        activities_dict[pidkey] = activity_name
        activities[activity_name] = {"prov:type": {"$": "act"+str(counter_entity),"type": "xsd:string"}, \
          "prov:startTime": format_time(pidkey.split('.')[1]),
          "prov:endTime": format_time(db.Get('prv.pid.'+pidkey+'.iexit'))
        }
        mydict['activity'] = activities
        counter_entity = counter_entity+1

    # 'prv.pid.24245.1432324941848481.exec.1432324941985923' -> [22] '24246.1432324941985923'
    # using sample line, let's generate

    wasInformedBy_dict = {}
    wasInformedBy_cnt = 1
    # "wasInformedBy": {
    # "_:Infm2": {
    #     "prov:informant": "a1",
    #     "prov:informed": "a2"


    for pidkey in activities_dict:
        mylist = []
        for (k, v) in db.RangeIter(key_from='prv.pid.'+pidkey+'.exec', key_to='prv.pid.'+pidkey+'.execzzz'):
            new_v= [jkl for jkl in activities_dict if jkl.split('.')[0] ==v.split('.')[0]]
            mylist.append(activities_dict[new_v[0]])
            wasInformedBy_dict["_:Infm"+str(wasInformedBy_cnt)]= {
                "prov:informant":activities_dict[pidkey],
                "prov:informed":activities_dict[new_v[0]]
            }
            wasInformedBy_cnt += 1
        # print activities_dict[pidkey],' started ',str(mylist)

    # print wasInformedBy_dict
    mydict['wasInformedBy']=wasInformedBy_dict

    #[51] 'prv.iopid.53151.1423602527480983.1.1423602529111612' -> [29] '/usr/lib/python2.7/codecs.pyc'
    files={v:k for pidkey in activities_dict for (k, v) in db.RangeIter(key_from='prv.iopid.'+pidkey, key_to='prv.iopid.'+pidkey+'.z') if isFilteredPath(v) and k[-2:]!="fd"}

    # files = k:v, k = file/entity, v= list of activities which needed file k
    files={}
    for pidkey in activities_dict:
        for (k, v) in db.RangeIter(key_from='prv.iopid.'+pidkey, key_to='prv.iopid.'+pidkey+'.z'):
            if isFilteredPath(v) and k[-2:]!="fd":
                if files.get(v,None) is None:
                    files[v]=[]
                # add only unique 'prv.iopid.24243.1432324940044957.1' from 'prv.iopid.24243.1432324940044957.1.1432324940223477'
                compare = '.'.join(k.split('.')[:5])
                boolWillAdd = True
                for prviopid_key in files[v]:
                    # print "compare ",compare, " with ",prviopid_key
                    if prviopid_key.startswith(compare):
                        boolWillAdd = False
                if boolWillAdd:
                    files[v].append(k)

    #pprint(files)

    entities_dict = {} #file_name : entity_name
    counter_entity=1
    counter_used = 1
    counter_was_generated_by = 1

    for  str_path in files:

        # ignore font files; needs rework
        if str_path.startswith('/home/cristian/.cache/fontconfig'):
            continue

        # ignore ptu scripts
        #if str_path.startswith('/home/cristian/git_proj_cv/tanu_tmp/provenance-to-use-tmp'):
        #    continue

        #ignore files which  contain unicode characters
        if unicode(str_path, errors='ignore') != unicode(str_path, errors='replace'):
            continue


        # print "--- Activity records for file ",str_path
        # print files[str_path]

        # pidkey = files[str_path]
        for pidkey in files[str_path]:

            keys = pidkey.split('.')
            # now, it looks like this:
            #['prv', 'iopid', '53151', '1423602527480983', '1', '1423602528323494']
            #/usr/lib/python2.7/_abcoll.py

            # populate entity fields
            entities = mydict.get('entity',{})

            str_datetime = format_time(keys[-1])
            entity_name = geounit_namespace+":en"+str(counter_entity)+"--"+str_path.replace(':','_')
            str_uuid = keys[-1]+str_path+version

            entities[entity_name] = {geounit_namespace+':creationTime':{'$':str_datetime,"type": "xsd:string"}, \
                                     geounit_namespace+':UUID':{"$": str_uuid,"type": "xsd:string"},\
                                     geounit_namespace+':version':{"$": version,"type": "xsd:string"}\
                                    }

            mydict['entity'] = entities
            entities_dict[str_path] = entity_name

            #ignore dumb activities (without fields)
            activity_name =activities_dict.get(keys[2]+'.'+keys[3],"")

            if activity_name == "":
                continue

            # create used / generated_by
            entity_name = entities_dict[str_path]
            activity_key = 'prov:activity'
            entity_key = 'prov:entity'
            if keys[4] == '1':
                used = mydict.get('used',{})
                used_name="_:u"+str(counter_used)
                used[used_name] = {activity_key:activity_name, \
                                   entity_key:entity_name}
                mydict['used'] = used
                counter_used+=1
            elif keys[4] in ['2','3']:
                wasgeneratedby = mydict.get('wasgeneratedby',{})
                wgb_name="_:wGB"+str(counter_was_generated_by)
                wasgeneratedby[wgb_name] = {activity_key:activity_name, \
                                            entity_key:entity_name}
                mydict['wasgeneratedby'] = wasgeneratedby
                counter_was_generated_by += 1
            counter_entity = counter_entity+1

    return mydict


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Converts content from LevelDB to JSON format')

    parser.add_argument('--input', dest='input_file', default=default_input_file,
                       help='input file - LevelDB database; default=%s'%default_input_file)
    parser.add_argument('--output', dest='output_file', default=default_output_file,
                       help='output file - will have the result in JSON format; default=%s'%default_output_file)
    args = parser.parse_args()

    #input file is LevelDB database file
    graph_dict = create_graph(args.input_file)

    #print json.dumps(mydict, indent=4, sort_keys=True)
    with open(args.output_file, 'w') as outfile:
        json.dump(graph_dict, outfile, sort_keys = True, indent = 4)
