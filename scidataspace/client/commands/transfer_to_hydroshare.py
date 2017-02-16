import hs_restclient as hsrc
import getpass 
import os

def authenticate():
    user_name = raw_input('User name: ')
    passwd  = getpass.getpass()
    auth = hsrc.HydroShareAuthBasic(username=user_name, password=passwd) 
    hs = hsrc.HydroShare(auth=auth)
    #del passwd
    return hs,user_name

def create_keywords():
    keywords = []
    n = raw_input('number of keywords to be added to the resource: ') 
    for i in range(1, int(n)+1):
        keywords.append(raw_input('keyword '+str(i)+":"))
    return keywords

def resource_type():
    L = ['GenericResource','ModelInstanceResource','ModelProgramResource','NetcdfResource','RasterResource','RefTimeSeries','SWATModelInstanceResource','TimeSeriesResource','ToolResource']
    print ""
    print "the following resource types are available."
    for i in range(len(L)):
        print str(i+1)+") "+L[i]
    k = raw_input('select by choosing a numerical value: ')
    return L[int(k)-1]

def package_to_tar_file_path():
    packid = raw_input('enter package hash id: ')
    user_dir = os.path.expanduser("~") #example: /home/ubuntu as string
    os.system("tar -czf "+user_dir+"/.gdclient/packages/docker_images/"+packid+".tar.gz "+user_dir+"/.gdclient/packages/"+packid)
    print ""
    print "The package has been compressed."
    return user_dir+"/.gdclient/packages/docker_images/"+packid+".tar.gz"

def create_new_resource(hs):
    abstract = raw_input('enter abstract: ')
    title = raw_input('title: ')
    keywds = create_keywords()
    rtype = resource_type()
    fpath = package_to_tar_file_path()
    resource_id = hs.createResource(rtype, title, resource_file=fpath, keywords=keywds, abstract=abstract)
    answ = raw_input('do you want to make this resource/geounit public (y/n): ')
    if answ == "y":    
       hs.setAccessRules(str(resource_id), public=True)
    else:
       hs.setAccessRules(str(resource_id), public=False)
    print "resource/geounit was created"
    os.system("rm -r "+fpath)
    
def show_all_my_resources(hs, my_user_name):        
    G = hs.getResourceList(creator=my_user_name) #G is a Python generator
    cnt = 1
    print "title : resource id"
    print "-------------------"
    for g in G:
        print str(cnt)+".", g[u'resource_title'], ": ", g[u'resource_id']
        cnt = cnt + 1

def main_menu():
    print "select action from the menu"
    print "1) create a new resource/geounit"
    print "2) explore an existing resource/geounit"
    a = raw_input('selection: ')
    return a

def show_resource_content(hs):
    a = ""
    resource_id = raw_input('enter resource id: ')
    print ""
    print "for main folder, press enter"
    path_name = raw_input("for a specific folder, enter path, or press enter for main folder: ")
    if len(a) > 0:
       folder_contents_json = hs.getResourceFolderContents(resource_id, pathname=path_name)
       print "                      Content Listing"
       print ""
       print "Resource ID: " 
       print folder_contents_json[u'resource_id']
       print "--"
       F = folder_contents_json[u'files']
       print "Files:"
       if len(F) == 0:
          print "this folder does not include any files"
          print "files might be located in subfolders (if present)"
       else:
          for i in range(len(F)):
              print F[i]
       print "--"
       FL = folder_contents_json[u'folders']
       print "Folders:"
       if len(FL) == 0:
          print "content does not include any folders"
       else:
          for i in range(len(FL)):
              print FL[i]
       print "--"
       p = folder_contents_json[u'path']
       print "Path:"
       if p == ".":
          print "main resource folder"
       else:
          print p
    else:
       folder_contents_json = hs.getResourceFolderContents(resource_id, '.')
       print "                      Content Listing"
       print ""
       print "Resource ID: "
       print folder_contents_json[u'resource_id']
       print "--"
       F = folder_contents_json[u'files']
       print "Files:"
       if len(F) == 0:
          print "this folder does not include any files"
          print "files might be located in subfolders (if present)"
       else:
          for i in range(len(F)):
              print F[i]
       print "--"
       FL = folder_contents_json[u'folders']
       print "Folders:"
       if len(FL) == 0:
          print "content does not include any folders"
       else:
          for i in range(len(FL)):
              print FL[i]
       print "--"
       p = folder_contents_json[u'path']
       print "Path:"
       if p == ".":
          print "main resource folder"
       else:
          print p

def transfer_to_hydroshare_server():    
    "transfering the geounit package"
    print ""
    print "****************************************************************"
    print "*                    hydroshare server access point            *"
    print "****************************************************************"
    print ""
    print "please log-in to your hydroshare account"
    aut = authenticate()
    hs = aut[0]
    my_user_name = aut[1]
    choice = main_menu()
    if choice == '1':
       create_new_resource(hs) 
    if choice == '2':
       a1 = raw_input('do you want a listing of your existing resources/geounits? (y/n): ')
       if a1 == 'y':
          show_all_my_resources(hs, my_user_name)
       a2 = raw_input('do you want to list content of a resource/geounit? (y/n): ')
       if a2 == 'y':
          show_resource_content(hs)
