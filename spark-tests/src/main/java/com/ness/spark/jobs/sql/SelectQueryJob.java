package com.ness.spark.jobs.sql;

import com.ness.spark.jobs.sql.config.ConnectionBean;
import com.ness.spark.jobs.sql.config.SQLJobContext;
import com.ness.spark.jobs.sql.config.QueryJobConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Created by p3700622 on 30/03/16.
 */
public class SelectQueryJob extends KryoSerializer {

    public static String SELECT_10 = "SELECT c_name FROM customers limit 10";
    public static final String INNER_SELECT = "(SELECT c_name FROM ral.customer where c_region='AFRICA')";
    public static String SELECT_COUNT = "select count(*) from customers";
    public static String SELECT_COUNT_MIN_RTRIM_UPPER =
            "(SELECT min (c_custkey) as minkey,rtrim( upper(c_name) ) trimname FROM ral.customer where c_region='AFRICA' group by c_name, c_custkey)";
    public static String SELECT_MIN_RTRIM_UPPER =
            "(SELECT min (c_custkey) as minkey,rtrim( upper(c_name) ) trimname FROM customers where c_region='AFRICA' group by c_name, c_custkey)";
    SQLContext sqlContext;

    public SelectQueryJob(SparkConf conf) {
        super(conf);
    }


    public void processSelectQuery(JavaSparkContext ctx) {
        sqlContext = getSqlContext(ctx, QueryJobConstants.CUSTOMER_TABLE);
        processQuery(ctx, SELECT_10);
    }

    public void processSelectCountQuery(JavaSparkContext ctx) {
        sqlContext = getSqlContext(ctx, INNER_SELECT);

        DataFrame dataFrame = sqlContext.sql(SELECT_COUNT);
        List<Row> customerList = dataFrame.collectAsList();
        for (Row row : customerList) {
            System.out.println("Count: " + row.getLong(0));
        }



    }
    public void processSelectCountFUNCQuery(JavaSparkContext ctx) {
        sqlContext = getSqlContext(ctx, SELECT_COUNT_MIN_RTRIM_UPPER);
        DataFrame dataFrame = sqlContext.sql(SELECT_COUNT);

        List<Row> customerList = dataFrame.collectAsList();
        for (Row row : customerList) {
            System.out.println("Count: " + row.getLong(0));
        }



    }

    public void processSelectFUNCQuery(JavaSparkContext ctx) {
        sqlContext = getSqlContext(ctx, QueryJobConstants.CUSTOMER_TABLE);
        DataFrame dataFrame = sqlContext.sql(SELECT_MIN_RTRIM_UPPER);
        List<Row> customerList = dataFrame.collectAsList();
        for (Row row : customerList) {
            System.out.println("Key: " + row.getInt(0));
            System.out.println("Name: " + row.getString(1));
        }



    }




    private void processQuery(JavaSparkContext ctx, String query) {
        DataFrame dataFrame = sqlContext.sql(query);

        List<Row> customerList = dataFrame.collectAsList();
        for (Row row : customerList) {
            System.out.println("Customer: " + row.getString(0));
        }



    }

    private SQLContext getSqlContext(JavaSparkContext ctx, String dTable) {
        SQLJobContext sqlJobContext = new SQLJobContext(ctx, new ConnectionBean(dTable, QueryJobConstants.REDSHIFT_DRIVER_JAR, QueryJobConstants.REDSHIFT_JDBC41_DRIVER, QueryJobConstants.REDSHIFT_CONNECTION_URL));
       sqlJobContext.registerTable(QueryJobConstants.CUSTOMER_TABLE,"customers");
        return sqlJobContext.getSqlContext();
    }


}
