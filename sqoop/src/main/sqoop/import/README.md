# Sqoop simple import

## Create table

Run the `create-table.sql` file in a Postgres database. It will drop-create the `import_table` table and insert a
couple of rows in it.

## Run Sqoop import

Edit the `import.sh` file and add the database connection details at the top of the file. Set the database name,
username and password.

Run the `import.sh` file. It will call Sqoop to import the previously created table into HDFS.

## Check results

You can do an `hdfs hdfs dfs -ls /tmp/sqoop_import` to check the generated files. To view a file's contents
do a `hdfs dfs -cat /tmp/sqoop_import/<file_name>`.
