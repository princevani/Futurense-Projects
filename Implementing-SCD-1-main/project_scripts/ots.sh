# -------- Shebang Statement -------- 

#!/bin/bash

# -------- Defining parameter source path --------

. /home/saif/cohort_f11/project_scripts/env/project_one_parameters.prm

# __________________________________________________________________________________ LOCAL FILE SYSTEM ______________________________________________________________________________

# -------- Checking if project directory already exists --delete and re-create dir --------

if test -d ${PROJECT_DIR}
then
	rm -r ${PROJECT_DIR}
fi

mkdir ${PROJECT_DIR}

# -------- Checking and Creating csv, archive and log directories in project dir,  writing logs to the log file --------

# Creating Directories
mkdir ${LOG_DIR}
mkdir ${CSV_DIR}
mkdir ${ARCHIVE_DIR}
mkdir ${PASSWORD_DIR}

#Creating empty password file
`touch ${PASSWORD_DIR}/db_pass.pwd`

# Writing changes to log file
echo "[${DATE_TODAY}] LFS-DIRECTORY > ${PROJECT_DIR} has been created successfully.\n" >> ${LOG_FILE_NAME}
echo "[${DATE_TODAY}] LFS-DIRECTORY > ${LOG_DIR} has been created successfully.\n" >> ${LOG_FILE_NAME}
echo "[${DATE_TODAY}] LFS-DIRECTORY > ${CSV_DIR} has been created successfully.\n" >> ${LOG_FILE_NAME}	
echo "[${DATE_TODAY}] LFS-DIRECTORY > ${ARCHIVE_DIR} has been created successfully.\n" >> ${LOG_FILE_NAME}
echo "[${DATE_TODAY}] LFS-DIRECTORY > ${PASSWORD_DIR} has been created successfully.\n" >> ${LOG_FILE_NAME}
echo "[${DATE_TODAY}] LFS-PASSWORD FILE > ${PASSWORD_DIR}/db_pass.pwd has been created successfully.\n" >> ${LOG_FILE_NAME}

# __________________________________________________________________________________ MYSQL  ______________________________________________________________________________

# -------- Connecting to db, creting a database, creating a table, writing logs to the log file --------

# Checking if database already exists --drop databse and re-create it
DBS=`mysql -u${USERNAME} -p${PASSWORD} -Bse 'show databases;'| egrep -v 'information_schema|mysql'`
for db in ${DBS}; 
do 
	if [ "${db}" = "${DB_NAME}" ] 
	then 
		mysql -u${USERNAME} -p${PASSWORD} -e "drop database ${DB_NAME};"
		break
	fi
done

# Creating database
mysql -u${USERNAME} -p${PASSWORD} -e "create database ${DB_NAME};"

# Creating tables > csv_data, r_data
mysql -u${USERNAME} -p${PASSWORD} ${DB_NAME} -e "create table ${TABLE_NAME}(custid integer(10),
username varchar(30), quote_count varchar(30), ip varchar(30), entry_time varchar(30),
prp_1 varchar(30), prp_2 varchar(30), prp_3 varchar(30), ms varchar(30),
http_type varchar(30), purchase_category varchar(30), total_count varchar(30),
purchase_sub_category varchar(30), http_info varchar(30),
status_code integer(10), update_time datetime default now());"

mysql -u${USERNAME} -p${PASSWORD} ${DB_NAME} -e "create table ${TABLE_NAME2}(custid integer(10),
username varchar(30), quote_count varchar(30), ip varchar(30), entry_time varchar(30),
prp_1 varchar(30), prp_2 varchar(30), prp_3 varchar(30), ms varchar(30),
http_type varchar(30), purchase_category varchar(30), total_count varchar(30),
purchase_sub_category varchar(30), http_info varchar(30),
status_code integer(10), update_time datetime);"

# Writitng to the log file
echo "[${DATE_TODAY}] MYSQL DATABASE > DATABASE ${DB_NAME} has been created successfully.\n" >> ${LOG_FILE_NAME}
echo "[${DATE_TODAY}] MYSQL DATABASE > TABLE ${TABLE_NAME} has been created successfully.\n" >> ${LOG_FILE_NAME}
echo "[${DATE_TODAY}] MYSQL DATABASE > TABLE ${TABLE_NAME2} has been created successfully.\n" >> ${LOG_FILE_NAME}

# ____________________________________________________________________ HADOOP DISTRIBUTED FILE SYSTEM ________________________________________________________________________

# -------- Checking if project directory already exists --delete and re-create dir --------

if test -d ${OP_DIR}/${DB_NAME}
then
        rm -r ${OP_DIR}/${DB_NAME}
fi

# Creating directory project_one and project_one/csv_data into hdfs

`hadoop fs -mkdir -p ${OP_DIR}/${DB_NAME}/${TABLE_NAME}`
`hadoop fs -mkdir -p ${OP_DIR}/${DB_NAME}/${TABLE_NAME}_copy`

# Writitng to the log file
echo "[${DATE_TODAY}] HDFS-DIRECTORY > ${OP_DIR}/${DB_NAME} has been created successfully.\n" >> ${LOG_FILE_NAME}
echo "[${DATE_TODAY}] HDFS-DIRECTORY > ${OP_DIR}/${DB_NAME}/${TABLE_NAME} has been created successfully.\n" >> ${LOG_FILE_NAME}
echo "[${DATE_TODAY}] HDFS-DIRECTORY > ${OP_DIR}/${DB_NAME}/${TABLE_NAME}_copy has been created successfully.\n" >> ${LOG_FILE_NAME}


# __________________________________________________________________________________ Sqoop Job ______________________________________________________________________________

# creating sqoop job for data insertion from mysql table to HDFS

for job in `sqoop job --list`
do
        if [ ${job} = 'project_one_append_job' ]
        then
                `sqoop job --delete project_one_append_job`
        fi
done

# Sqoop Job
sqoop job --create project_one_append_job -- import \
       --connect jdbc:mysql://${HOST}:${PORT_NO}/${DB_NAME}?useSSL=false \
       --username ${USERNAME} --password-file ${PASSWORD_FILE} \
       --query 'select custid, username, quote_count, ip, entry_time, prp_1, prp_2, prp_3, ms, http_type, purchase_category, total_count, purchase_sub_category, http_info, status_code, update_time from csv_data where $CONDITIONS' \
       -m 1 --target-dir ${OP_DIR}/${DB_NAME}/${TABLE_NAME} \
       --incremental append --check-column update_time --last-value "1900-01-01 00:00:00.000"

# Writitng to the log file
echo "[${DATE_TODAY}] SQOOP JOB > Sqoop Job 'project_one_append_job' has been created successfully.\n" >> ${LOG_FILE_NAME}

# __________________________________________________________________________________ HIVE ______________________________________________________________________________

# Checking if database already exists --drop databse and re-create it
# `nohup hive --service metastore &`

DBS=`hive -e 'show databases;'`
for db in ${DBS};
do
        if [ "${db}" = "${DB_NAME}" ]
        then
               	hive -e "drop database if exists ${DB_NAME} CASCADE;"
                break
        fi
done

# Creating database
hive -e "create database ${DB_NAME};"

# Creating tables > managed table > csv_data, external_table > csv_data_ext, alternate table > csv_data_alt
hive -e "create table if not exists ${DB_NAME}.${TABLE_NAME}(custid int, username string, quote_count string, ip string, entry_time string,
prp_1 string, prp_2 string, prp_3 string, ms string, http_type string, purchase_category string, total_count string,
purchase_sub_category string, http_info string, status_code int, update_time timestamp)
row format delimited fields terminated by ',';"

hive -e "create external table if not exists ${DB_NAME}.${TABLE_NAME}_ext(custid int, username string, quote_count string, ip string, entry_time string, 
prp_1 string, prp_2 string, prp_3 string, ms string, http_type string, purchase_category string, total_count string, 
purchase_sub_category string, http_info string, status_code int, day int, update_time timestamp)
partitioned by(year string, month string)
row format delimited fields terminated by ',';"

hive -e "create table if not exists ${DB_NAME}.${TABLE_NAME}_alt(custid int, username string, quote_count string, ip string, entry_time string,
prp_1 string, prp_2 string, prp_3 string, ms string, http_type string, purchase_category string, total_count string,
purchase_sub_category string, http_info string, status_code int, update_time timestamp)
row format delimited fields terminated by ',';"

# Writitng to the log file
echo "[${DATE_TODAY}] HIVE DATABASE > DATABASE ${DB_NAME} has been created successfully.\n" >> ${LOG_FILE_NAME}
echo "[${DATE_TODAY}] HIVE DATABASE > MANAGED TABLE ${TABLE_NAME} has been created successfully.\n" >> ${LOG_FILE_NAME}
echo "[${DATE_TODAY}] HIVE DATABASE > EXTERNAL TABLE ${TABLE_NAME}_ext has been created successfully.\n" >> ${LOG_FILE_NAME}
echo "[${DATE_TODAY}] HIVE DATABASE > MANAGED TABLE ${TABLE_NAME}_alt has been created successfully.\n" >> ${LOG_FILE_NAME}

# ___________________________________________________________________________________________________________________________________________________________________________
