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

            r, data = datasetClient.create_dataset(catalog_id,dict(name=geounit_name))
            geounit_id = data['id']

            return geounit_name, geounit_id, ""
        else:
            return None, None, "cannot understand geounit name"

    elif cmd_2 == "delete":
        pass
        #if geounit_id is None:
        #    return None, None, "cannot use geounit name"

    else:
        # geounit something
        return None, None, "usage: geounit [start <gounit name>|delete]"

    return None, None, ""

