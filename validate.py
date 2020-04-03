#!/usr/bin/env python
import snowflake.connector
import os
import csv
import yaml


##
## Reference: https://docs.snowflake.com/en/user-guide/python-connector-example.html
## AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
## AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
## PASSWORD = os.getenv('SNOWSQL_PWD')
## WAREHOUSE = os.getenv('WAREHOUSE')
##

def file_row_count(fname, first_row_is_header):
    count = len(open(fname).readlines())
    if first_row_is_header:
        return count-1
    else:
        return count

def process_definition(file_row):
    if file_row:
        print('Validating Table ['+file_row[0]+']')
        cs.execute("select 'TABLE', count(*) from " + file_row[0])
        tables = cs.fetchall()
        for row in tables:
            print("\t" + row[0] + ": " + str(row[1]))
        print("\t" + file_row[1] + ": " + str(file_row_count(file_row[1], bool(file_row[2]))))
    else:
        print("There is an empty row here")

def config_data_setup(config_file_path):
    with open (config_file_path) as config_yml:
        return yaml.load(config_yml, Loader=yaml.FullLoader)

# Gets the version

config_data = config_data_setup('config.yml')

ctx = snowflake.connector.connect(
    user=config_data['snowflake']['user'],
    password=config_data['snowflake']['password'],
    account=config_data['snowflake']['account'],
    role=config_data['snowflake']['role'],
    warehouse = config_data['snowflake']['warehouse']
    # schema='xxxxx',
    # database='xxxxx'
    )
cs = ctx.cursor()
try:
    with open(config_data['app']['definition']) as csvfile:
        csv_file = csv.reader(csvfile, delimiter=',')
        for file_row in csv_file:
            process_definition(file_row)
except OSError as err:
    print("OS error: {0}".format(err))
except snowflake.connector.errors.OperationalError as err:
    print("OperationalError: {0}".format(err))
finally:
    cs.close()
ctx.close()