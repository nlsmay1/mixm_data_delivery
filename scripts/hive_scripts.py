import cx_Oracle

#Declare Input Parameter File with list of tables
tableslist="Tableslist.txt"

#Output Files
createHiveStmt="createHiveORCStmt.txt"
#createHiveORCingestion="createHiveORCingestionStmt.txt"

#Declare Connection Parameter here
connstr='xxxxx/xxxxx@xxxxx.xxxxx.xxxxx:xxxxx/xxxxx.xxxxx.xxxxx:xxxxx'
conn = cx_Oracle.connect(connstr)
cur = conn.cursor()

#Define the Input and Output Files here
input_file = open(tableslist,'r')
createHiveStmt_file=open(createHiveStmt,'w')
#HiveORCingestion_file=open(createHiveORCingestion,'w')
print (input_file)
#Decleare arraylist for getting ColumnNames and Types as well as Column Names
getColValTypes = []
getColNames=[]

#Define the Schema Details
#hiveAVROSchema="AVRODB."
hiveORCSchema="D_NATIVE_ZONE."

#Define table Creation Scripts (defaults)
tablecreation=["CREATE EXTERNAL TABLE","  ROW FORMAT DELIMITED FIELDS TERMINATED BY "," STORED AS TEXTFILE"]

#ingestionStmt = ["SET tez.queue.name=default;SET hive.exec.dynamic.partition = true;SET hive.exec.dynamic.partition.mode=nonstrict;SET hive.execution.engine=tez;SET mapreduce.framework.name=yarn-tez;SET hive.exec.max.dynamic.partitions=100000;SET hive.exec.max.dynamic.partitions.pernode=100000;","INSERT TABLE ","PARTITION (LOAD_YEAR,LOAD_MONTH,LOAD_DATE) ",",LOAD_YEAR,LOAD_MONTH,LOAD_DATE from "]


#Build the Create Statement for all tables from the sources: 
for tableName in input_file:
    sqlStmt='SELECT * FROM {usertable} where rownum < 2'.format(usertable=tableName)
    print(tableName)
    a=cur.execute(sqlStmt)
    print(sqlStmt)
    tableSchema=cur.description

    for i in tableSchema:
        columnName=str(i[0])
        columnType=str(i[1])
        if 'NUMBER' in columnType:
            columnType="INT"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'STRING' in columnType:
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'FIXED_CHAR' in columnType:
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'TIMESTAMP' in columnType:
            columnType="TIMESTAMP"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'CLOB' in columnType:
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'BLOB' in columnType:
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'VARCHAR' in columnType:
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'CHAR' in columnType:
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'DATE' in columnType:
            columnType="DATE"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'RAW' in columnType:
            columnType="STRING"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'FLOAT' in columnType:
            columnType="DECIMAL"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        elif 'LONG' in columnType:
            columnType="DOUBLE"
            getColValTypes.append(columnName+' '+columnType)
            getColNames.append(columnName)
        else:
            getColValTypes.append(columnName+' '+"STRING")
            getColNames.append(columnName)
            tableName=tableName.split(sep='.')
    createHiveStmt_file.write(tablecreation[0]+' '+hiveORCSchema+tableName+'('+','.join(getColValTypes)+')'+tablecreation[1]+r"','"+tablecreation[2]+' location '+"'"+'/user/incoming_data/native_zone/symphony/mixm/initial/'+tableName+"'"+';'+'\n'+'\n')
    #HiveORCingestion_file.write(ingestionStmt[0]+' '+ingestionStmt[1]+hiveORCSchema+tableName[1]+ingestionStmt[2]+'SELECT '+','.join(getColNames)+ingestionStmt[3]+hiveAVROSchema+tableName[1]+';'+'\n'+'\n')
    getColValTypes=[]
    getColNames=[]
createHiveStmt_file.close()
#HiveORCingestion_file.close()
cur.close()
conn.close()
