# Sqoop simple export

## Create table

Run the `create-table.sql` in a Postgres database. It will drop-create the `export_table` table.

## Upload data to HDFS

SSH into the cluster and run the `upload-data.sh` to upload the `data.csv` file to HDFS. By default, it will place the
file in
`/tmp/swoop_export`.

## Run Sqoop export

Now, edit the `export.sh` file and add the database connection details at the top of the file. Set the database name,
username and password.

Run the `export.sh` file.  It will call Sqoop to export the uploaded `data.csv` file into the previously created DB
table.

## Check results

Log in the database and run a `SELECT * FROM export_table`. It must contain all the data in the `data.csv` file.
