# Sqoop simple exercise

## Create table

Run the `create-table.sql` in the database. It will create the `mytable` table and insert a couple rows in it.

## Run simple Sqoop import

SSH into the cluster and run the `simple-import.sh` file. It will call Sqoop to import the previously created table into HDFS.

## Run parametrized Sqoop import

SSH into the cluster and run the `import-with-params.sh` file. It uses the `sqoop.params` file to load the parameters for importing the previously created table.

