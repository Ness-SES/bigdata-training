package com.ness.spark.jobs.sql.config;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;

import java.io.Serializable;

/**
 * Created by p3700622 on 28/03/16.
 */
public class QueryJobConstants {
    public static final String APP_NAME = "JavaSparkSQL";
    public static final String SPARK_MASTER_URL = "spark://127.0.0.1:7077";
    public static final String CUSTOMER_TABLE = "ral.customer";
    public static final String SUPPLIER_TABLE = "ral.supplier";
    public static String CUSTOMER_JOIN_TABLE = "(select c_name, s_name from ral.customer, ral.supplier where c_city = s_city) ";
    public static final String REDSHIFT_JDBC41_DRIVER = "com.amazon.redshift.jdbc41.Driver";
    public static final String SQL_USERNAME = "ness";
    public static final String SQL_PWD = "Ness2016";
    public static final String REDSHIFT_CONNECTION_URL = "jdbc:redshift://wmg.cqfubjdrthl5.us-west-2.redshift.amazonaws.com:5439/wmg?user=" + SQL_USERNAME + "&password=" + SQL_PWD;
    public static final String REDSHIFT_DRIVER_JAR = "/Users/p3700622/nesswork/tools/RedshiftJDBC41-1.1.13.1013.jar";
    public static final String CLASSES_JAR = "/Users/p3700622/nesswork/projects/sparkProjects/target/spark-POC-1.0-SNAPSHOT.jar";



}


