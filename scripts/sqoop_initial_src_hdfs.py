import pymysql
import cx_Oracle
import subprocess
import logging
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
import getpass
import json
from  create_query_oracle import create_query
#from read_max_load_date import max_load_date


#Connect to the database in mysql for auditing the changes. Currently only last date is added into the audit table.
mysql = pymysql.connect(host='xxxxx',
                             user='xxxxx',
                             password='xxxxx',
                             db='xxxxx',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)





#Read the source tables to be extracted from the json file
with open('param_tables.json') as json_param_file:
    table_name = json.load(json_param_file)

table_name = table_name['source_tables']

#Read the environment varaibles from the json file
with open('param_env_variable.json') as env_param_file:
    env_param = json.load(env_param_file)

oracle_url = env_param['oracle_connection']
username = env_param['username']
source_schema = env_param ['source_schema']
password_alias = env_param['password_alias']
alias_provider = env_param['alias_provider']
target_dir = env_param['target_dir']
oracle_url = oracle_url[0]
username = username[0]
source_schema = source_schema [0]
password_alias = password_alias[0]
alias_provider = alias_provider[0]
target_dir = target_dir[0]


# Function to run Hadoop command
def run_unix_cmd(args_list):
    print('Running system command:{0}'.format('     '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess .PIPE, universal_newlines=True)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err

# Create Sqoop Job to load data from source into HDFS Target Directory

def sqoop_job(table_name):
    #query = ('"select a.*, '+' current_timestamp, '+ "'NLSMAY1'" + ' from '  + source_schema+'.'+table_name +' a '+ ' where $CONDITIONS"')
    query = create_query(source_schema+"."+table_name)
    #last_update_date = max_load_date(table_name)
    cmd = ['sqoop', 'import', '-Dhadoop.security.credential.provider.path='+alias_provider, '--connect', oracle_url, '--username', username,'--password-alias', password_alias, '-m', '1', '--as-textfile','--target-dir', target_dir+'/'+table_name,  '--query',query]
    cmd2 = ['hdfs', 'dfs', '-rm',  target_dir+'/'+table_name+'/'+'_SUCCESS']
    print(cmd)
    print('Removing Success Flag from ' +target_dir+'/'+table_name)
    print(cmd2)
    (ret, out, err) = run_unix_cmd(cmd)
    print(ret, out, err)
    (ret, out, err) = run_unix_cmd(cmd2)
    print(ret, out, err)
    #try:
        #with mysql.cursor() as cursor:
            #sql = ("insert into sqoop_audit values ('"+str(last_update_date)+"'"+" , "+"'"+table_name+"'" + " , "+'current_timestamp'+")")
            #print(sql)
            #cursor.execute(sql)
            #mysql.commit()
    #finally:
        #pass  

    if ret == 0:
        logging.info('Success.')
    else:
        logging.info('Error.')

#Run the job in sequence for each table. 
for i in table_name:
    sqoop_job(i)

mysql.close()
