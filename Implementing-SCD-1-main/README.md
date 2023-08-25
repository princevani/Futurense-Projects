# Implementing SCD-1
Automate the data insertion day wise from csv using shell script.

-----------------------------------------------------------------------------------------------------------------------------------
>> Project Workflow:
-----------------------------------------------------------------------------------------------------------------------------------

![Project_One_Big_Data](https://user-images.githubusercontent.com/107995858/175769545-3c4bfcfa-5e8f-470b-bc59-57bf0dc90ec3.jpeg)

>>Steps:
1) Taking Day_1.csv from the archieve dirwctory to csv_data directory in project dir.
2) Laod Day_1.csv data from LFS (Local File System) to Mysql Database table-1.
3) Using Sqoop Job incremental append/lastmodified mode insert the data from Mysql Database Table-1 to HDFS (Hadoop Distributed File System) 
4) Insert data from HDFS to Hive Database Managed Table-1.
5) Create Hive External Table-2, perform dynamic partition on year & month.
6) Perform the SCD-1 Logic to insert the data into Hive External Table-2 from Hive Managed Table-1.
7) Find out the latest data modified/inserted today, insert the data into Hive Managed Table-3.
8) Using Sqoop job, export the data from Hive Managed Table-3 to Mysql Database Table-2.
9) Perform the data checksum on MySql Database Table-1 and Table-2.
10) Repeat all the steps for Day-2, Day-3 and so on.
11) Write every action/changes to log file.
