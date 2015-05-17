from catalog_wrapper import *


if __name__ == "__main__":

	#Test info
	catalog_id = '17'
	dataset_id = '54'
	member_id = '100'
	destination_endpoint = 'go#ep2'
	
	#Store authentication data in a local file
	token_file = os.getenv('HOME','')+"/.ssh/gotoken.txt"
	wrap = CatalogWrapper(token_file=token_file)

 #EXAMPLES FOR RAY   
 #    #CREATE DATASET   -- RETURNS the parent catalog ID and the resultant dataset ID
	# try:
 #        _,result = wrap.catalogClient.create_dataset(catalog_id, annotation_arg)
 #        print "%s,%s"%(catalog_id,result['id'])
 #    except Exception, e:
 #        print e		

 #     #ADD A DATASET ANNOTATION
 #      try:
 #      	_,response = wrap.catalogClient.add_dataset_annotations(catalog_id, dataset_id, annotation_arg)
 #      	print response   
 #      except Exception, e:
 #      	print e

 #    #CREATE MEMBERS   -- RETURNS the parent catalog ID, parent dataset ID, and the resultant member ID    
 #    try:
	#     _,result = wrap.catalogClient.create_members(catalog_id, dataset_id, member_arg)
	#     print "%s,%s,%s"%(catalog_id,dataset_id,result['id'])
	# except Exception, e:
	#     print e
	#     return False   

	# #CREATE MEMBER ANNOTATION     
	# try:    
	# 	_,response = wrap.catalogClient.add_member_annotations(catalog_id,dataset_id,member_id,annotation_arg)
 #   	except Exception, e:
 #   		print response  

 #   	#CREATE ANNOTATION DEF
 #   	#@value_type are from the enum {'enum': ['text', 'int8', 'float8', 'boolean', 'timestamptz', 'date']}
 #    _,response = wrap.catalogClient.create_annotation_def(catalog_id=catalog_id, annotation_name=annotation_arg, 
 #                                                        value_type=value_arg, multivalued=bool_multivalue_arg)
 #    print response	

 #    #GET CATALOG ANNOTATION DEFS
 #    _,annotation_defs = wrap.catalogClient.get_annotation_defs(catalog_id)


	####### OLDER EXAMPLES #######
	#CASE: List all catalogs
	_,catalogs = wrap.catalogClient.get_catalogs()

	print "== Making a _CATALOG_ Menu == "
	for catalog in catalogs:
		#Make menu item with associated ID as the "key" value
		try:
			print "\t%s)\t%s  (%s)"%(catalog['id'], catalog['config']['name'],catalog['config']['owner'])
		except Exception, e:
			print e




	#CASE: 
	#Return a list of datasets within a catalog
	#Use this to build a select list, menu, etc
	_,catalog_datasets = wrap.catalogClient.get_datasets(catalog_id)

	print "\n\n== Making a _DATASET_ Menu for Catalog - %s =="%(catalog_id)
	print "** Note that the data structure is slightly different **"
	for dataset in catalog_datasets:
		#Make menu item with associated ID as the "key" value
		try:
			print "\t%s)\t%s  (%s)"%(dataset['id'], dataset['name'],dataset['owner'])
		except Exception, e:
			pass


	#CASE:
	#Return a list of all data members within a catalog->dataset
	#Use this to build a select list, menu, etc		
	_,dataset_members = wrap.catalogClient.get_members(catalog_id,dataset_id)
	print "\n\n== Making a _DATA MEMBER_ Menu for Catalog - %s Dataset - %s =="%(catalog_id,dataset_id)

	for member in dataset_members:
		#{u'dataset_reference': [u'54'], u'id': 74, u'data_type': u'file', u'data_uri': u'globus://go#ep1/share/godata/file1.txt'}
		try:
			print "\t %s)\t%s\tTYPE:%s PARENT DATASET:%s"%(member['id'],member['data_uri'],member['data_type'],member['dataset_reference'])
		except Exception, e:
			pass


	#CASE: Transfer all files from a Dataset within a Catalog to a destination endpoint
	print "\n\n== Testing Transfer =="
	wrap.set_destination_endpoint(destination_endpoint)    #This is the endpoint name where you want to transfer the files to
	wrap.transfer_members(catalog_id,dataset_id)

