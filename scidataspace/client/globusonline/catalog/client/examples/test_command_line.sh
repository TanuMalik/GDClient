#!/bin/bash          
echo "==Checking get_catalogs=="
python catalog.py get_catalogs
python catalog.py -t get_catalogs

echo "==Checking get_datsets=="
python catalog.py get_datasets 17
python catalog.py -t get_datasets 17

#echo "Checking CREATE Catalog"
#python catalog.py create_catalog '{"config": {"name": "*DELETE* SH Script TEST"}}'

echo "==Checking CREATE Dataset=="
python catalog.py create_dataset 17 '{"name":"New Dataset"}'

echo "==Checking get_dataset_members=="
python catalog.py get_dataset_members 17 54
python catalog.py -t get_dataset_members 17 54

echo "==Checking get_dataset_annotations=="
python catalog.py get_dataset_annotations 17 100  
python catalog.py -t get_dataset_annotations 17 100   

echo "==Checking query_dataset_name=="
python catalog.py query_datasets 17 name LIKE %New%    
python catalog.py -t query_datasets 17 name LIKE %New%

