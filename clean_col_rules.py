# JMP-278 Automation to deploy columns for RAW to CLEAN mappings 
import sys
import json

modelName = sys.argv[1]
jsonFile = sys.argv[2]
schemaList = []

with open(jsonFile) as f:
    for jsonObj in f:
        schemaDict = json.loads(jsonObj)
        schemaList.append(schemaDict)
#         print(schemaDict)

# print(schemaList)
# Clean Rules:  
# 1. Remove _c for all columns  2. for timestamps - add _utc_ts , for boolean - add _flag
def clean_cols(listOfDict):
    schema_new = []
    for i in listOfDict:
        if i['data_type'].lower() == 'boolean':
            if i['col_name'].lower().endswith('_c') == True:
                s = i['col_name'][:-2] + '_flag'
                schema_new.append(s)
            else:
                schema_new.append(i['col_name'] + '_flag')
        elif i['data_type'].lower() == 'timestamp':
            if i['col_name'].lower().endswith('_c') == True:
                s = i['col_name'][:-2] + '_utc_ts'
                schema_new.append(s)
            else:
                schema_new.append(i['col_name'] + '_utc_ts')
        else:
            if i['col_name'].lower().endswith('_c') == True:
                schema_new.append(i['col_name'][:-2])
            else:
                schema_new.append(i['col_name'])
    return schema_new

schemaClean = clean_cols(schemaList)

# Generationg DBT Model - <model_name.sql>
original_stdout = sys.stdout
with open(modelName+'_clean.sql', 'w') as f:
	sys.stdout = f
	print("SELECT " +'\n'+  ", \n".join(str('\t'+ x) for x in schemaClean) + '\n' +'FROM ' + modelName)
	sys.stdout = original_stdout
