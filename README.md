# SCD_1
Using MYSQL,SQOOP,HADOOP-HDFS,HIVE created this project for implementing scd_1 logic with out sqoop incremental append job


Implementing SCD1 logic in Data Warehouse for POC

What exactly is SCD1 ?
Ans : SCD - Slowly Changing Dimension,
In a Type 1 SCD the new data overwrites the existing data. Thus the existing data is lost as it is not stored anywhere else. 
This is the default type of dimension you create

Process flow :
step1: Automated the script for transfering files from LFS to MYSQL
step2: Created a Sqoop Job to send data from mysql HDFS (Hadoop File System).
step3: Created Internal Table in Hive and then transferred data from HDFS to hive table
step4: Then Created External table with dynamic partition and load data into external table from Internal table
 step5: Used SCD1 logic to get the updated data.
 step6 :Finally exported updated data from hive external table to SQL for data reconcialiation
