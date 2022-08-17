import sqlalchemy
import sqlalchemy_bigquery
import lookml
import sqlalchemy as SA
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
import json
from typing import Union

tables_in_database, tables_in_project = [], []

#Create connection to BQ
engine = create_engine(
            'bigquery://my_great_bg_project', 
            credentials_info=json.load(open('credentials.json','r'))
            )
# Initialize the inspector
inspector = SA.inspect(engine)

#loop over tables in DB
for t in inspector.get_table_names('vision'):
    tables_in_database.append(t)

#Initialize pyLookML project (see lookml docs, this is local files for simplicity.
# Also can be done over github https or ssh. See https://pylookml.readthedocs.io/en/latest/)
myProject = lookml.Project(path='example_project')

# Loop over views in LookML project
for vf in myProject.view_files():
    for viewObject in vf.views:
        tables_in_project.append(viewObject.sql_table_name.value.strip())

# Obtain New tables to target generation
new_tables = set(tables_in_database) - set(tables_in_project)

#Function to convert sqlalchemy to lookml types
def sa_to_lookml_type(
        satype: Union[
            sqlalchemy.Integer
            ,sqlalchemy.String
            ,sqlalchemy.TIMESTAMP
            ,sqlalchemy.Float
        ]
        ) -> str:
    typeMap = {
         sqlalchemy.Integer:'number'
        ,sqlalchemy.String:'string'
        ,sqlalchemy.TIMESTAMP: 'date'
        ,sqlalchemy.DATE: 'date'
        ,sqlalchemy.Float: 'number'
    }
    return typeMap[type(satype)]

#functions for pretty casing
def dimName(col:dict)->str:
    if col['name'] == 'name':
        return 'name_'
    else:
        return col['name'].lower().replace(' ','')
label = lambda c: c['name'].replace('_',' ').title()

for table in new_tables:
    schemaName = table.split('.')[0] 
    tableName = table.split('.')[1]
    newView = lookml.View(f'{tableName}')
    newView + f'sql_table_name: {schemaName}.{tableName} ;;'
    columns = inspector.get_columns(tableName,schemaName)
    for column in columns:
        newDim = f'''
            dimension: {dimName(column)} {{
                label: "{label(column)}"
                type: {sa_to_lookml_type(column['type'])}
                sql: ${{TABLE}}.{column['name']} ;;
            }}
        '''
        newView + newDim
    newFile = myProject.new_file(f'{tableName}.view.lkml')
    newFile + newView
    newFile.write()