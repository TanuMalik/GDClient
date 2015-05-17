#!python
# Note: The line above is modified by setup.py

import sys
import json
import traceback
from optparse import OptionParser

from globusonline.catalog.client.catalog_wrapper import *
from globusonline.catalog.client.operators import Op
from globusonline.catalog.client.rest_client import RestClientError

print_text = False  #Variable used to decide whether output should be in JSON (False) or limited plain text (True)
default_catalog = None
force = False
name_mode = False
show_output = True
short_format = False
use_log_files = False

def check_environment():
    global show_output
    global use_log_files
    global default_catalog

    if os.getenv('GCAT_SHOW_OUTPUT') == '0':
        show_output = False
    if os.getenv('GCAT_USE_LOG_FILES') == '1':
        use_log_files = True
    default_catalog = os.getenv('GCAT_DEFAULT_CATALOG_ID')

def is_true(arg):
    return (arg.lower() == "true")

def format_catalog_text(the_catalog):
    catalog_description = ''
    catalog_name = ''
    try:
        catalog_description = the_catalog['config']['description']
    except KeyError:
        catalog_description = ''
    try:
        catalog_name = the_catalog['config']['name']
    except KeyError:
        catalog_name = 'no catalog name'
    return "%s)\t%s - [%s] - %s"%(the_catalog['id'], catalog_name, the_catalog['config']['owner'], catalog_description)

def format_dataset_text(the_dataset):
    global short_format
    dataset_labels = ''
    if short_format:
        return the_dataset['id']
    try:
        dataset_labels = ','.join(the_dataset['label'])
    except:
        dataset_labels = 'no labels'
    try:
        dataset_name = the_dataset['name']
    except KeyError:
        dataset_name = 'no dataset name'
    return "%s) %s - [%s] - <%s>"%(the_dataset['id'], dataset_name, the_dataset['owner'], dataset_labels)

def format_member_text(member):
    if short_format:
        result = "%s %s %s" % \
             (member['id'], member['data_type'], member['data_uri'])
    else:
        member_references = ''
        try:
            member_references = ','.join(member['dataset_reference'])
        except:
            member_references = 'no references'
        result = "%s) ref:%s - %s - %s" % \
            (member['id'], member_references,
             member['data_type'], member['data_uri'])
    return result

def make_annotation_dict(args):
    result = dict()
    for arg in args:
        tokens = arg.partition(':')
        if tokens[1] is "":
            print "malformed annotation: ", arg
            return None
        key = tokens[0]
        value = tokens[2]
        result[key] = value
    return result

# Lookup name:<name>; return the dataset ID
# Raises LookupError
def resolve_dataset_name(catalog_id, name):
    if name is None:
        raise LookupError("No name provided for lookup")
    selector_list = [("name",Op["LIKE"],'%'+name+'%')]
    _,result = client.get_datasets(catalog_id, selector_list=selector_list)
    if len(result) == 0:
        raise LookupError("Nothing found for name:"+name)
    elif len(result) > 1:
        raise LookupError("Found multiple (%i) entries for name:%s" %(len(result), name))
    dataset = result[0]
    id = dataset['id']
    return id

# Lookup name:<name>; return the dataset ID
# Raises LookupError
def resolve_member_name(catalog_id, name):
    if name is None:
        raise LookupError("No name provided for lookup")

    selector_list = [("name",Op["LIKE"],'%'+name+'%')]
    _,result = client.get_datasets(catalog_id, selector_list=selector_list)
    if len(result) == 0:
        raise LookupError("Nothing found for name:"+name)
    elif len(result) > 1:
        raise LookupError("Found multiple (%i) entries for name:%s" %\
                          len(result), name)
    dataset = result[0]
    id = dataset['id']
    return id

class ArgsException(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

class UsageException(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

def check_arg_count(args_given, args_required):
    if len(args_given) != args_required:
        raise ArgsException \
        ("required arguments: %i, given arguments: %i" %\
                       (args_required, len(args_given)))

# Many commands allow an optional catalog argument first:
def check_arg_count_cat(args_given, args_required):
    if len(args_given) != args_required:
        raise ArgsException \
        ("after the optional catalog argument:\n" +
         "required arguments: %i, given arguments: %i" %\
                       (args_required, len(args_given)))

# Either return the default catalog or pop the first argument as the
# catalog ID
def pop_catalog(args):
    if default_catalog:
        catalog_arg = default_catalog
    else:
        if len(args) > 0:
            catalog_arg = args.pop(0)
        else:
            raise UsageException("No catalog specified!")
    return catalog_arg

def get_catalogs(args):
    check_arg_count(args, 0)
    try:
        _, catalog_list = client.get_catalogs()
        if not show_output:
            return True
        if print_text:
            print "============================================================"
            print "*More detailed catalog information available in JSON format*"
            print 'ID) Catalog Name - [Owner] - Catalog Description'
            print "============================================================"
        for catalog in catalog_list:
            if print_text:
                print format_catalog_text(catalog)
            else:
                print json.dumps(catalog_list)
    except KeyError:
        # print e
        return False
    return True

def create_catalog(args):
    check_arg_count(args, 1)

    if args[0][0] == '{':
        arg_dict = json.loads(args[0])
    else:
        s = "{\"config\": {\"name\":\"" + args[0] + "\"}}"
        arg_dict = json.loads(s)
    try:
        #print "CREATE CATALOG - %s"%(arg_dict)
        try:
            _,catalog_list = client.create_catalog(arg_dict)
            if show_output:
                print catalog_list['id']
        except Exception, e:
            if show_output:
                print e
            return False
        return True
    except (IndexError, AttributeError):
        if show_output:
            print "==================ERROR===================="
            print "Invalid Arguments passed for create_catalog"
            print "create_catalog accepts one argument 1) catalog property list"
            print "Example: python catalog.py create_catalog '{\"config\": {\"name\": \"JSON CAT NEW\"}}'"
            print "==========================================="
    except KeyError, e:
        if show_output:
            print e
        return False
    else:
        return False

def delete_catalog(args):
    #@arg[0] = catalog ID -- INT
    #@arg[1] = verify -- True to verify deletion
    # We do not allow a default catalog here to avoid errors
    check_arg_count(args, 2)
    try:
        if args[0] != '' and is_true(args[1]):
            if show_output:
                print "DELETE CATALOG - Catalog ID:%s"%(args[0])
                client.delete_catalog(args[0])
            return True
    except IndexError:
            if show_output:
                print "==================ERROR===================="
                print "Invalid Arguments passed for delete_catalog"
                print "delete_catalog accepts two arguments 1) the catalog ID and 2) a verification to delete (i.e. true)"
                print "Example: python catalog.py delete_catalog 1234 true"
                print "==========================================="
    except KeyError, e:
        if show_output:
            print e
        return False
    else:
        return False

def create_dataset(args):
    #Arguments f(catalog_id, annotation_list)
    #annotation list -- text string '{"name":"New Dataset"}'

    catalog_arg = pop_catalog(args)
    if len(args) != 1:
        raise UsageException("create_dataset: Requires dataset name!")
    annotation_arg = args.pop(0)
    if annotation_arg[0] == '{':
        # Received JSON
        annotation_dict = json.loads(annotation_arg)
    else:
        annotation_dict = {"name":annotation_arg }
    # else:
    #     # Received list of KEY:VALUE
    #     annotation_dict = make_annotation_dict([annotation_arg])

    _,result = client.create_dataset(catalog_arg,annotation_dict)
    if show_output:
        print result['id']

def get_datasets(args):
    #Arguments f(catalog_id)
    catalog_arg = pop_catalog(args)

    _,cur_datasets= client.get_datasets(catalog_arg)

    if show_output:
        if print_text is True:
            if not short_format:
                print "============================================================"
                print "*More detailed dataset information available in JSON format*"
                print "ID) Dataset Name  - [Owner] - <Datset Labels>"
                print "============================================================"
            for dataset in cur_datasets:
                print format_dataset_text(dataset)
        else:
            print json.dumps(cur_datasets)
    return True

def create_annotation_def(args):
    #Arguments f(catalog_id, annotation_name, value_type)
    multivalue_arg = "False"
    response = ""

    def error(msg):
        raise UsageException("get_annotation_defs: " + msg)

    catalog_arg = pop_catalog(args)
    if len(args) < 2:
        error("usage: <catalog>? <annotation> <value> " +
              "<multivalue>? <multivalue_arg>?")
    annotation_arg = args[0]
    value_arg = args[1]
    try:
        multivalue_arg = args[2]
        multivalue_arg = args[3]
    except IndexError:
        pass

    bool_multivalue_arg = is_true(multivalue_arg)
    try:
        response = client.create_annotation_def \
            (catalog_id=catalog_arg, annotation_name=annotation_arg,
             value_type=value_arg,   multivalued=bool_multivalue_arg)
    except Exception as e:
        if re.search("Tag.*is already defined", str(e)):
            if force:
                # The tag is already defined: ignore this error
                response = ""
            else:
                raise e
        else:
            raise e
    return response

def delete_annotation_def(args):
    #Arguments f(catalog_id, annotation_name)

    catalog_arg = pop_catalog(args)
    if len(args) != 1:
        error("usage: <catalog>? <annotation>")
    annotation_arg = args[0]
    response = client.delete_annotation_def \
        (catalog_id=catalog_arg, annotation_name=annotation_arg)
    return response

def get_annotation_defs(args):
    # Arguments: catalog_id

    catalog_arg = pop_catalog(args)

    _,annotation_defs = client.get_annotation_defs(catalog_arg)

    if show_output:
        if print_text:
            for annotation in annotation_defs:
                print "%s %s" % (annotation["name"], annotation["value_type"])
        else:
            print json.dumps(annotation_defs)

def get_dataset_annotations(args):
    #@arg[0] = catalog_id -- INT
    #@arg[1] = dataset id -- INT

    def error(msg):
        raise UsageException("get_dataset_annotations: " + msg)

    try:
        catalog_arg = pop_catalog(args)
        check_arg_count_cat(args, 1)
        dataset_arg = args.pop(0)
        if name_mode:
            dataset_arg = resolve_dataset_name(catalog_arg, dataset_arg)
    except IndexError:
        if show_output:
            print "==================ERROR===================="
            print "Invalid Arguments passed for get_dataset_annotations"
            print "get_dataset_annotations accepts two arguments"
            print "1) the catalog ID and 2) the dataset ID"
            print "Example: python catalog.py get_annotation_defs 17"
            print "==========================================="
        return False
    except KeyError, e:
        if show_output:
            print 'KeyError:',e
        return False
    except LookupError as e:
        print e
        return False

    if catalog_arg and dataset_arg:
        _,tmp_result = client.get_dataset_annotations(catalog_arg,dataset_arg,annotation_list=['annotations_present'])
        if len(tmp_result) == 0:
            print "No annotations found"
            return True
        _,dataset_annotations = client.get_dataset_annotations(catalog_arg,dataset_arg,annotation_list=tmp_result[0]['annotations_present'])
    else:
        if show_output:
            print 'Invalid arguments passed'
        return False

    if print_text is True:
        if show_output:
            for record in dataset_annotations:
                for annotation_key in record:
                    value = record[annotation_key]
                    if isinstance(value, list) and len(value) == 1:
                        value = value[0]
                    print "%s:%s"%(annotation_key, value)
    else:
        if show_output:
            print json.dumps(dataset_annotations)
    return True

def add_dataset_annotation(args):
    catalog_arg = None
    dataset_arg = None
    existing_annotations = []

    default_annotation_type = 'text'
    def error(msg):
        raise UsageException("add_dataset_annotation: " + msg)

    try:
        if default_catalog:
            catalog_arg = default_catalog
        else:
            if len(args) > 0:
                catalog_arg = args.pop(0)
                print catalog_arg
            else:
                error("Insufficient arguments!")
        dataset_arg = args.pop(0)
        if name_mode:
            dataset_arg = resolve_dataset_name(catalog_arg, dataset_arg)
        if args[0][0] == '{':
            # Received JSON
            annotation_dict = json.loads(args[0])
        else:
            # Received list of KEY:VALUE
            annotation_dict = make_annotation_dict(args)

        _,result = client.get_annotation_defs(catalog_arg)
        for item in result:
            existing_annotations.append(item['name'])

        for item in annotation_dict:
            if item not in existing_annotations:
                client.create_annotation_def \
                            (catalog_id=catalog_arg, annotation_name=item,
                             value_type=default_annotation_type,   multivalued=True)

        client.add_dataset_annotations(catalog_arg,dataset_arg,annotation_dict)

    except IndexError:
        if show_output:
            print "==================ERROR===================="
            print "Invalid Arguments passed for add_dataset_annotation"
            print "add_dataset_annotation accepts three arguments"
            print "1) the catalog ID and 2) the dataset ID 3) Annotation list"
            print "Example: python catalog.py add_dataset_annotation 17 54 '{\"test-annotation\":\"true\",\"material\":\"copper\"}'"
            print "==========================================="
        return False
    except KeyError, e:
        if show_output:
            print 'KeyError:',e
        return False
    except LookupError as e:
        print e
        return False
    if catalog_arg and dataset_arg and annotation_dict:
        #print "ADD DATASET TAG - Catalog ID:%s Dataset ID:%s Annotations:%s",(args[0],args[1],args[2])
        client.add_dataset_annotations(catalog_arg,dataset_arg,annotation_dict)
        return True
    else:
        print "add_dataset_annotation: did not receive all required arguments!"
        return False

def delete_dataset_annotation(args):
    catalog_arg = pop_catalog(args)
    if len(args) < 3:
        raise UsageException("delete_dataset_annotation: usage: " +
                             "<catalog> <dataset> <annotation_name> <annotation_value>")
    dataset_arg = args.pop(0)
    if name_mode:
        dataset_arg = resolve_dataset_name(catalog_arg, dataset_arg)
    annotation_name = args.pop(0)
    annotation_value = args.pop(0)
    print catalog_arg + ' ' + dataset_arg + ' ' +annotation_name+':'+annotation_value
    _,result = client.delete_dataset_annotation(catalog_arg, dataset_arg, annotation_name, annotation_value)
    print result

def delete_dataset(args):
    #@arg[0] = catalog ID -- INT
    #@arg[1] = dataset ID -- INT
    #@arg[2] = verify -- True to verify deletion

    def error(msg):
        raise UsageException("delete_dataset: " + msg)

    catalog_arg = pop_catalog(args)

    if len(args) == 0:
        error("No dataset given!")
    if len(args) == 1:
        error("You did not confirm the delete!")
    if len(args) > 2:
        error("Too many arguments!")
    dataset_arg = args[0]
    verify_arg = args[1]

    if name_mode:
        dataset_arg = resolve_dataset_name(catalog_arg, dataset_arg)

    if is_true(verify_arg):
        client.delete_dataset(catalog_arg,dataset_arg)
    else:
        raise UsageException("delete_dataset: You did not confirm the delete!")
    return True

def add_dataset_acl(args):
    #@arg[0] = catalog ID -- INT
    #@arg[1] = dataset ID -- INT
    #@arg[2] = acl

    def error(msg):
        raise UsageException("add_dataset_acl: " + msg)

    def usage():
        error("usage: <catalog ID>? <dataset ID> <principal> " +
              "<principal_type> <permission>")

    if len(args) < 4:
        usage()

    catalog_arg = pop_catalog(args)

    if len(args) < 4:
        usage()

    dataset_arg = args.pop(0)
    acl_arg = args[0]

    try:
        if acl_arg[0] == '{':
            acl = json.loads(acl_arg)
        else:
            acl = { "principal":      args[0],
                    "principal_type": args[1],
                    "permission":     args[2]}
    except IndexError:
        print "add_dataset_acl: missing argument!"
        usage()

    client.add_dataset_acl(catalog_arg, dataset_arg, acl)
    return True

def add_dataset_acl_recursive(args):
    #@arg[0] = catalog ID -- INT
    #@arg[1] = acl
    catalog_arg = pop_catalog(args)
    if len(args) < 1:
        raise UsageException("add_dataset_acl_recursive: " +
                             "requires ACL: <principal> <type> <perms>!")
    acl_arg = args[0]
    if acl_arg[0] == '{':
        acl = json.loads(acl_arg)
    else:
        acl = { "principal":      args[0],
                "principal_type": args[1],
                "permission":     args[2]}
    _,result = client.get_datasets(catalog_arg)
    for dataset in result:
        client.add_dataset_acl(catalog_arg, dataset['id'], acl)

def print_acl(acl):
    print "%s %s %s" % \
        (acl["principal_type"], acl["principal"], acl["permission"])

def print_acls(acls):
    for acl in acls:
        print_acl(acl)

def get_dataset_acl(args):
    #@arg[0] = catalog ID -- INT
    #@arg[1] = dataset ID -- INT
    catalog_arg = pop_catalog(args)
    check_arg_count_cat(args, 1)
    dataset_arg = args[0]
    _,response = client.get_dataset_acl(catalog_arg, dataset_arg)

    if show_output:
        if print_text:
            print_acls(response)
        else:
            print response

def create_members(args):
    catalog_arg = pop_catalog(args)
    if len(args) < 2:
        raise UsageException("create_members: " +
                             "requires dataset and members!")
    dataset_arg = args.pop(0)
    if name_mode:
        dataset_arg = resolve_dataset_name(catalog_arg, dataset_arg)
    member_arg = args[0]
    if member_arg[0] == '{':
        members = json.loads(member_arg)
    else:
        if len(args) != 2:
            raise UsageException("requires: <data_type> <data_uri>")
        members = { "data_type": args[0],
                    "data_uri":  args[1]}
    _,result = client.create_members(catalog_arg,dataset_arg,members)
    if show_output:
        print result['id']
    return True

def get_dataset_members(args):
    #@arg[0] = catalog ID -- INT
    #@arg[1] = dataset ID -- INT
    catalog_arg = pop_catalog(args)
    check_arg_count_cat(args, 1)
    dataset_arg = args[0]

    if name_mode:
        dataset_arg = resolve_dataset_name(catalog_arg, dataset_arg)

    if catalog_arg and dataset_arg:
        _,cur_members = client.get_members(catalog_arg,dataset_arg)
        if print_text:
            if show_output:
                if not short_format:
                    print "============================================================"
                    print "*More detailed member information available in JSON format*"
                    print 'ID) Reference Dataset - Member Type - URI'
                    print "============================================================"
                for member in cur_members:
                    print format_member_text(member)
        else:
            if show_output:
                print json.dumps(cur_members)
            return True

def add_member_annotation(args):
    #@arg[0] = catalog_id -- INT
    #@arg[1] = dataset id -- INT
    #@arg[2] = annotation list -- text string '{new-attribute:value}'
    catalog_arg = pop_catalog(args)
    if len(args) < 3:
        raise UsageException("add_member_annotation: usage: " +
                             "<dataset> <member> <annotation>")
    dataset_arg = args.pop(0)
    if name_mode:
        dataset_arg = resolve_dataset_name(catalog_arg, dataset_arg)
    member_arg = args.pop(0)
    annotation_arg = args[0]
    if annotation_arg[0] == '{':
        annotation = json.loads(annotation_arg)
    else:
        annotation = make_annotation_dict(args)
    if catalog_arg and dataset_arg and member_arg and annotation_arg:
        client.add_member_annotations \
                     (catalog_arg, dataset_arg, member_arg, annotation)
    return True

def get_member_annotations(args):
    #@arg[0] = catalog_id -- INT
    #@arg[1] = dataset id -- INT
    #@arg[2] = members id -- INT
    catalog_arg = pop_catalog(args)
    if len(args) < 2:
        raise UsageException("get_member_annotations: usage: " +
                             "<dataset> <member>")
    dataset_arg = args.pop(0)
    if name_mode:
        dataset_arg = resolve_dataset_name(catalog_arg, dataset_arg)
    member_arg = args.pop(0)

    _,response = client.get_all_member_annotations \
                         (catalog_arg, dataset_arg, member_arg)

    member_arg = int(member_arg)
    if print_text:
        for record in response:
            # if record["id"] == member_arg:
                for annotation_key in record:
                    print "%s:%s"%(annotation_key, record[annotation_key])

    else:
        print response
    return True

def get_existing_member_annotations(args):
    catalog_arg = pop_catalog(args)
    if len(args) < 2:
        raise UsageException("get_existing_member_annotations: usage: " +
                             "<dataset> <member>")
    dataset_arg = args.pop(0)
    if name_mode:
        dataset_arg = resolve_dataset_name(catalog_arg, dataset_arg)
    member_arg = args.pop(0)

    # Retrieve a list of annotations present on the given member
    request_string = "/catalog/id=%s/dataset/id=%s/member/id=%s/annotation/annotations_present"%(catalog_arg, dataset_arg, member_arg)
    _,result = client._request('GET', request_string)
    print "result: ", result
    if len(result) == 0:
        print "No annotations."
        return True
    annotations_present = result[0]['annotations_present']

    # With the list of annotations present, retrieve the values
    _,result = client.get_member_annotations(catalog_arg, dataset_arg, member_arg, annotations_present)
    if print_text:
        for record in result:
            lengths = map(len, record.keys())
            m = max(lengths)
            for key in record:
                k = (key+":").ljust(m+2)
                sys.stdout.write(k)
                if isinstance(record[key], list):
                    print record[key][0]
                else:
                    print record[key]
        return True
    else:
        print json.dumps(result)
    return True

def delete_member_annotation(args):
    catalog_arg = pop_catalog(args)
    if len(args) < 4:
        raise UsageException("delete_member_annotation: usage: " +
                             "<catalog> <dataset> <member> <annotation_name> <annotation_value>")
    dataset_arg = args.pop(0)
    if name_mode:
        dataset_arg = resolve_dataset_name(catalog_arg, dataset_arg)
    member_arg = args.pop(0)
    annotation_name = args.pop(0)
    annotation_value = args.pop(0)
    _,result = client.delete_member_annotation(catalog_arg, dataset_arg, member_arg, annotation_name, annotation_value)
    print result


def convert_op(op):
    if op == '==':
        return 'EQUAL'
    elif op == '!=':
        return 'NOT_EQUAL'
    elif op == '>':
        return 'GT'
    elif op == '>=':
        return 'GEQ'
    elif op == '<':
        return 'LT'
    elif op == '<=':
        return 'LEQ'
    # The double tilde avoids shell expansion
    elif op == '~' or op == '~~':
        return 'LIKE'
    else:
        return op

def query_datasets(args):
    catalog_arg = pop_catalog(args)

    if len(args) != 2 and len(args) != 3:
        print "usage: query_datasets <field> <operator> <value>?"
        sys.exit(1)
    field_arg = args[0]
    operator_arg = args[1]
    if len(args) > 2:
        value_arg = args[2]

    op = convert_op(operator_arg)

    try:
        if len(args) == 2:
            L = [(field_arg,Op[op])]
        else:
            L = [(field_arg,Op[op],value_arg)]
        _,result = client.get_datasets(catalog_arg,
                                                   selector_list=L)
    except KeyError:
        print 'Unknown query operator %s ' % op
        print 'Valid query operators are: '
        print ' '.join(Op.keys())
        print 'with obvious aliases:'
        print '== > >= < <='
        print 'and aliases ~ or ~~ for LIKE'
        return False
    except ValueError as e:
        print str(e)
        return False

    if print_text is True:
        if show_output:
            for dataset in result:
                print format_dataset_text(dataset)
        return True
    else:
        if show_output:
            print json.dumps(result)
        return True

def create_token_file():
    wrap.create_token_file()
    return True

def delete_token_file():
    if(wrap.check_authenticate()):
        if show_output:
            print '===Deleting access token==='
        wrap.delete_token_file()
    else:
        if show_output:
            print 'No authentication token detected'
            return False

def query_members(args):
    catalog_arg = pop_catalog(args)
    check_arg_count_cat(args, 4)
    dataset_arg = args[0]
    field_arg = args[1]
    operator_arg = args[2]
    value_arg = args[3]

    try:
        tmp_selector_list = [(field_arg,Op[operator_arg],value_arg)]
        if show_output:
            print catalog_arg
            print dataset_arg
            print tmp_selector_list
            _,result = client.get_members(catalog_arg,dataset_arg,selector_list=tmp_selector_list)
    except KeyError:
        if show_output:
            print 'Unknown query operator %s -- Known query Operators are'%operator_arg
            print Op.keys()
        return False

    if print_text:
        if show_output:
            for dataset in result:
                print format_member_text(dataset)
    else:
        if show_output:
            print json.dumps(result)
    return True



# Set up commands:
commands_catalog = [ "get_catalogs", "create_catalog", "delete_catalog",
                     "create_annotation_def", "delete_annotation_def",
                     "get_annotation_defs" ]
commands_dataset = [ "get_datasets", "create_dataset", "delete_dataset",
                     "add_dataset_annotation","add_dataset_annotation_force",
                     "get_dataset_annotations", "query_datasets",
                     "add_dataset_acl", "get_dataset_acl",
                     "add_dataset_acl_recursive", "delete_dataset_annotation" ]
commands_member  = [ "get_dataset_members", "create_members",
                     "add_member_annotation", "get_member_annotations",
                     "get_existing_member_annotations",
                     "delete_member_annotation" ]
commands_token   = [ "write_token", "delete_token_file" ]
commands = commands_catalog + commands_dataset + \
           commands_member  + commands_token

def describe_commands():
    print "Globus Catalog CLI Commands"
    print "---------------------------"
    print "Catalog commands:"
    print("\t"+"\n\t".join(commands_catalog))
    print "Dataset commands:"
    print("\t"+"\n\t".join(commands_dataset))
    print "Member commands:"
    print("\t"+"\n\t".join(commands_member))
    print "Token commands:"
    print("\t"+"\n\t".join(commands_token))

def dispatch_command(command, args):
    if not (command in commands):
        print "catalog.py: Invalid command:", command
        sys.exit(1)
    function = globals()[command]
    result = function(args)
    return result

def run_parser():
    parser = OptionParser()
    parser.add_option("-f", "--force",
                      action="store_true", dest="force", default=False,
                      help="allow problematic operations to complete silently")
    parser.add_option("-H", "--commands",
                      action="store_true", dest="help_commands", default=False,
                      help="list all commands")
    parser.add_option("-n", "--name",
                      action="store_true", dest="name_mode", default=False,
                      help="operate on names instead of dataset IDs")
    parser.add_option("-s", "--short",
                      action="store_true", dest="short_format", default=False,
                      help="generate plain text output in short format")
    parser.add_option("-t", "--text",
                      action="store_true", dest="print_text", default=False,
                      help="generate plain text output (default is JSON)")
    parser.add_option("-x",
                      action="store_false", dest="show_output", default=True,
                      help="generate no output")

    (options, args) = parser.parse_args()
    global force, name_mode, short_format, print_text, show_output
    force        = options.force
    name_mode    = options.name_mode
    short_format = options.short_format
    print_text   = options.print_text
    show_output  = options.show_output

    if options.help_commands:
        describe_commands()
        sys.exit(0)

    if len(args) == 0:
        print "No arguments!"
        print "Use -h for help."
        print ""
        parser.print_usage()
        sys.exit(1)

    return args

if __name__ == "__main__":

    selector_list = []

    # Store authentication data in a local file
    token_file = os.getenv('HOME','')+"/.ssh/gotoken.txt"
    wrap = CatalogWrapper(token_file=token_file)
    client = wrap.catalogClient

    args = run_parser()

    check_environment()
    the_command = args.pop(0)
    success = False
    try:
        success = dispatch_command(the_command, args)
    except ArgsException as e:
        print "Argument error in command:", the_command
        print "\t", str(e)
    except UsageException as e:
        print str(e)
    except RestClientError as e:
        # print "RestClientError"
        print e.body["message"]
    except Exception as e:
        print "Unknown exception!"
        print e
        traceback.print_exc()

    arg_list = ['python']+sys.argv
    log_string = ' '.join(arg_list)

    log_file = 'log/GlobusCatalog-log.txt'
    fail_log_file = 'log/GlobusCatalog-failed-log.txt'

    if use_log_files:
        with open(log_file, "a") as myfile:
            myfile.write(log_string+'\n')

    if success is False:
        if use_log_files:
            with open(fail_log_file, "a") as myfile:
                myfile.write(log_string+'\n')
        sys.exit(1)
