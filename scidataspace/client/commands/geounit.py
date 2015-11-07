from scidataspace.client.commands.util import UNDEFINED

#######################################
#   Parse geounit command
#   returns  (geounit_name, geounit_id, err_message)
#######################################
def parse_cmd_geounit(cmd_splitted, catalog_id, geounit_id, datasetClient):
    cmd_2 = cmd_splitted.get(1,"")
    if cmd_2 == "start":
        geounit_name = cmd_splitted.get(2,UNDEFINED)
        if geounit_name != UNDEFINED:
            geounit_name = geounit_name

            r, datasets = datasetClient.get_datasets(catalog_id)
            filtered_datasets = [x for x in datasets if x['name']==geounit_name]
            if len(filtered_datasets)<1:
                r, data = datasetClient.create_dataset(catalog_id,dict(name=geounit_name))
                geounit_id = data['id']
            elif len(filtered_datasets)== 1:
                geounit_id = filtered_datasets[0]['id']
            else:
                return UNDEFINED, None, "please choose another name; this one seems corrupted"

            return geounit_name, geounit_id, ""
        else:
            return UNDEFINED, None, "cannot understand geounit name"

    elif cmd_2 == "delete":
        pass
        #if geounit_id is None:
        #    return None, None, "cannot use geounit name"

    else:
        # geounit something
        return UNDEFINED, None, "usage: geounit [start <gounit name>|delete]"

    return UNDEFINED, None, ""

