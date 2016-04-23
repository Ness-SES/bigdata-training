# Sqoop simple export

## Create table

Run the `create-table.sql` in the database. It will drop-create the `export_table` table.

## Upload data to HDFS

Run the `upload-data.sh` to upload the `data.csv` file to HDFS.

## Run Sqoop export

SSH into the cluster and run the `export.sh` file. It will call Sqoop to export the uploaded `data.csv` file into the previously created DB table.
