from scidataspace.client.commands.util import UNDEFINED
from scidataspace.client.commands.util import is_geounit_selected

#######################################
#   Parse annotation
#######################################
def parse_cmd_annotate(cmd_splitted, catalog_id, geounit_id, datasetClient, db):
    if  not is_geounit_selected(geounit_id): return

    cmd_2 = cmd_splitted.get(1,"")

    # annotate geounit
    if cmd_2 == "geounit":
        for geo_prop_value in cmd_splitted[2:]:
            # print "xxx: ",geo_prop_value
            geo_property, geo_value = geo_prop_value.split(':')

            #creates a annotation definition; type="text, multivalue=True
            try:
                _, _ = datasetClient.create_annotation_def(catalog_id,geo_property,"text",True)
            except:
                print "annotation definition already exists"
                pass
            _, annotation = datasetClient.add_dataset_annotations(catalog_id,geounit_id, {geo_property: geo_value})
            print annotation['code']

    # annotate member
    elif cmd_2 == "member":
        member_name = cmd_splitted.get(2,UNDEFINED)
        if member_name != UNDEFINED:
            member_ids = [v for (k, v) in db.RangeIter(key_from='member.'+member_name, key_to='member.'+member_name+'zzz')]
            if len(member_ids) != 0:
                member_id=member_ids[0]
                for member_prop_value in cmd_splitted[3:]:
                    member_annotation_name, member_annotation_value = member_prop_value.split(':')

                    try:
                        _, annotation_def = datasetClient.create_annotation_def(catalog_id,member_annotation_name,"text",True)
                    except:
                        # print "annotation definition already exists"
                        pass
                    r, annotation = datasetClient.add_member_annotations(catalog_id,geounit_id, member_id,
                                                                        {member_annotation_name: member_annotation_value})
                    print annotation['code']
                print "ok"
            else:
                print "could not find member "+member_name

    # annotate something
    else:
        print "USAGE: annotate [geounit |member <member name>] property:value"
