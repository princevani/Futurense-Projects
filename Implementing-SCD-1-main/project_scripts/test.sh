# -------- Shebang Statement -------- 

#!/bin/bash

# -------- Defining parameter source path --------

. /home/saif/cohort_f11/project_scripts/env/project_one_parameters.prm

# Geeting latest csv data file

FILE_PATH=`ls -t -r ${CSV_DIR}/* | tail -1`
echo "'$FILE_PATH'"

# __________________________________________________________________________________ LFS > MYSQL ______________________________________________________________________________

# -------- Connecting to db, creting a batabase, Inserting latest data records into csv_data table, writing logs to the log file --------
mysql -u${USERNAME} -p${PASSWORD} ${DB_NAME} -e "truncate table ${TABLE_NAME};"
mysql -u${USERNAME} -p${PASSWORD} ${DB_NAME} -e "SET GLOBAL local_infile=1;"
mysql --local-infile=1 -u${USERNAME} -p${PASSWORD} ${DB_NAME} -e "LOAD DATA LOCAL INFILE '${FILE_PATH}' INTO TABLE ${TABLE_NAME} FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';"
mysql -u${USERNAME} -p${PASSWORD} ${DB_NAME} -e "alter table ${TABLE_NAME} drop column update_time;"
mysql -u${USERNAME} -p${PASSWORD} ${DB_NAME} -e "alter table ${TABLE_NAME} add column update_time datetime default now();"

# Writing changes to log file
echo "[${DATE_TODAY}}] LFS to MYSQL > MYSQL Table '${TABLE_NAME}' has been truncated successfully.\n" >> ${LOG_FILE_NAME}
echo "[${DATE_TODAY}}] LFS to MYSQL > New data has been inserted into ${TABLE_NAME} successfully.\n" >> ${LOG_FILE_NAME}

# ____________________________________________________________________ MYSQL > HADOOP DISTRIBUTED FILE SYSTEM _______________________________________________________________________

# Removing the existing dir
`hadoop fs -rm -r ${OP_DIR}/${DB_NAME}`

# Executing the sqoop job 'project_one_append_job'
sqoop job --exec project_one_append_job

# Copying the file into new dir
`hadoop fs -mkdir -p ${OP_DIR}/${DB_NAME}/${TABLE_NAME}_copy`
`hadoop fs -cp ${OP_DIR}/${DB_NAME}/${TABLE_NAME}/part-m-00000 ${OP_DIR}/${DB_NAME}/${TABLE_NAME}_copy`

# Writing changes to log file
echo "[${DATE_TODAY}}] SQOOP JOB > Sqoop job 'project_one_append_job' has been executed successfully.\n" >> ${LOG_FILE_NAME}
echo "[${DATE_TODAY}}] SQOOP JOB > Data insertion from MYSQL to HDFS is successfully completed.\n" >> ${LOG_FILE_NAME}

# __________________________________________________________________________________ HDFS > HIVE ______________________________________________________________________________

# Inserting data from HDFS to HIVE MANAGED TABLE 'csv_data'
hive -e "truncate table ${DB_NAME}.${TABLE_NAME};"
hive -e "load data inpath '${OP_DIR}/${DB_NAME}/${TABLE_NAME}' into table ${DB_NAME}.${TABLE_NAME}"

# Writing changes to log file
echo "[${DATE_TODAY}}] HDFS to HIVE > MANAGED TABLE '${TABLE_NAME}' has been truncated successfully.\n" >> ${LOG_FILE_NAME}
echo "[${DATE_TODAY}}] HDFS to HIVE > Data insertion from HDFS to MANAGED TABLE '${TABLE_NAME}' is successfully completed.\n" >> ${LOG_FILE_NAME}

# Inserting data from MANAGED TABLE 'csv_data' to EXTERNAL TABLE 'csv_data_ext'
if [ ${FILE_PATH} = '/home/saif/cohort_f11/project_one/csv/Day_1.csv' ]
then
	hive -e "set hive.exec.dynamic.partition=true;
	set hive.exec.dynamic.partition.mode=nonstrict;
	insert overwrite table ${DB_NAME}.${TABLE_NAME}_ext partition (year, month) select custid, username, quote_count, ip, entry_time, prp_1, prp_2, prp_3, ms, http_type, purchase_category,
	total_count, purchase_sub_category, http_info, status_code, cast(day(from_unixtime(unix_timestamp(entry_time, 'dd/MMM/yyyy'))) as int) as day, update_time, 
	cast(year(from_unixtime(unix_timestamp(entry_time, 'dd/MMM/yyyy'))) as string) as year, cast(month(from_unixtime(unix_timestamp(entry_time, 'dd/MMM/yyyy'))) as string) as month 
	from ${DB_NAME}.${TABLE_NAME};"

else
        hive -e "set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
	use ${DB_NAME};
	insert overwrite table ${TABLE_NAME}_ext partition (year, month) select custid, username, quote_count, ip, entry_time, prp_1, prp_2, prp_3, ms, http_type, purchase_category, total_count, purchase_sub_category, http_info, status_code, cast(day(from_unixtime(unix_timestamp(entry_time, 'dd/MMM/yyyy'))) as int) as day, update_time, cast(year(from_unixtime(unix_timestamp(entry_time, 'dd/MMM/yyyy'))) as string) as year, cast(month(from_unixtime(unix_timestamp(entry_time, 'dd/MMM/yyyy'))) as string) as month from ${TABLE_NAME} union select custid, username, quote_count, ip, entry_time, prp_1, prp_2, prp_3, ms, http_type, purchase_category, total_count,purchase_sub_category, http_info, status_code, cast(day(from_unixtime(unix_timestamp(entry_time, 'dd/MMM/yyyy'))) as int) as day, update_time, cast(year(from_unixtime(unix_timestamp(entry_time, 'dd/MMM/yyyy'))) as string) as year, cast(month(from_unixtime(unix_timestamp(entry_time, 'dd/MMM/yyyy'))) as string) as month from ${TABLE_NAME}_ext ce where ce.custid not in(select custid from ${TABLE_NAME});"

fi

# Writing changes to log file
echo "[${DATE_TODAY}}] HDFS to HIVE > Data insertion/updation from '${TABLE_NAME}' to EXTERNAL TABLE '${TABLE_NAME}_ext' is successfully completed.\n" >> ${LOG_FILE_NAME}

# Inserting data from 'csv_data_ext' to HIVE MANAGED TABLE 'csv_data_alt'
hive -e "truncate table ${DB_NAME}.${TABLE_NAME}_alt;"
if [ ${FILE_PATH} = '/home/saif/cohort_f11/project_one/csv/Day_1.csv' ]
then
	hive -e "load data inpath '${OP_DIR}/${DB_NAME}/${TABLE_NAME}_copy' into table ${DB_NAME}.${TABLE_NAME}_alt"

else
	hive -e "insert overwrite table ${DB_NAME}.${TABLE_NAME}_alt select custid, username, quote_count, ip, entry_time, prp_1, prp_2, prp_3, ms, http_type, purchase_category, total_count, 
	purchase_sub_category, http_info, status_code, update_time from ${DB_NAME}.${TABLE_NAME}_ext where update_time in(select update_time from ${DB_NAME}.${TABLE_NAME});"
fi

# Writing changes to log file
echo "[${DATE_TODAY}}] HDFS to HIVE > MANAGED TABLE '${TABLE_NAME}_alt' has been truncated successfully.\n" >> ${LOG_FILE_NAME}
echo "[${DATE_TODAY}}] HDFS to HIVE > Data insertion from '${TABLE_NAME}' to MANAGED TABLE '${TABLE_NAME}_alt' is successfully completed.\n" >> ${LOG_FILE_NAME}

# __________________________________________________________________________________ HIVE > MYSQL  ______________________________________________________________________________
#truncating mysql table 'r_data'
mysql -u${USERNAME} -p${PASSWORD} ${DB_NAME} -e "truncate table ${TABLE_NAME2};"

# Exporting new updated|inserted|latest version of data from HIVE table 'csv_data_alt' to MYSQL TABLE 'r_data' 
`sqoop export --connect jdbc:mysql://${HOST}:${PORT_NO}/${DB_NAME}?useSSL=False \
	--table ${TABLE_NAME2} \
	--username ${USERNAME} \
	--password-file ${PASSWORD_FILE} \
	--export-dir "/user/hive/warehouse/${DB_NAME}.db/${TABLE_NAME}_alt" \
	-- driver com.mysql.jdbc.Driver \
	--input-fields-terminated-by ',';`

# Writing changes to log file
echo "[${DATE_TODAY}}] HIVE to MYSQL > Data has been exported successfully from HIVE MANAGED TABLE '${TABLE_NAME}_alt'to MYSQL TABLE '${TABLE_NAME2}'.\n" >> ${LOG_FILE_NAME}

# __________________________________________________________________________________ MATCHING RECORDS ______________________________________________________________________________

# Matching the total records in both the MYSQL tables 'csv_data' and 'r_data', writing to the log file
data_count=`mysql -u${USERNAME} -p${PASSWORD} ${DB_NAME} -e "select count(1) from ${TABLE_NAME};"|tail -1`
data_count2=`mysql -u${USERNAME} -p${PASSWORD} ${DB_NAME} -e "select count(1) from ${TABLE_NAME2};"| tail -1`

# Writing changes to log file
if [ ${data_count} -eq ${data_count2} ]
then
	echo "[${DATE_TODAY}}] ALL RECORDS ARE SAME IN ${TABLE_NAME} AND ${TABLE_NAME2}.\n" >> ${LOG_FILE_NAME}
else
	echo "[${DATE_TODAY}}] ALL RECORDS ARE NOT SAME IN ${TABLE_NAME} AND ${TABLE_NAME2}.\n" >> ${LOG_FILE_NAME}
fi

# ______________________________________________________________________________________ END __________________________________________________________________________________________

# Day2: update> custid = 1
