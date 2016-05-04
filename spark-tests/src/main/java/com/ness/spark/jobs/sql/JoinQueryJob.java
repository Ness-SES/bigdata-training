package com.ness.spark.jobs.sql;

import com.ness.spark.jobs.sql.config.ConnectionBean;
import com.ness.spark.jobs.sql.config.SQLJobContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;
import java.util.List;
import static com.ness.spark.jobs.sql.config.QueryJobConstants.*;

/**
 * Created by p3700622 on 30/03/16.
 */
public class JoinQueryJob implements Serializable {

    public static final String SELECT_JOIN_10 = "SELECT c_name, s_name FROM customer,supplier where c_city = s_city limit 10";
    public static String JOIN_10 = "SELECT c_name, s_name FROM custSupSameCity limit 10";

    public void processJoinTableQuery(JavaSparkContext ctx) {


        SQLJobContext sqlJobContext = new SQLJobContext(ctx,
                new ConnectionBean(CUSTOMER_JOIN_TABLE, REDSHIFT_DRIVER_JAR, REDSHIFT_JDBC41_DRIVER, REDSHIFT_CONNECTION_URL));
        SQLContext sqlContext = sqlJobContext.getSqlContext();
        sqlJobContext.registerTable(CUSTOMER_JOIN_TABLE,"custSupSameCity");
        DataFrame dataFrame = sqlContext.sql(JOIN_10);

        List<Row> dataList = dataFrame.collectAsList();
        for (Row row : dataList) {
            System.out.println("Cust: "+ row.getString(0)+" -SUP" +row.getString(1));
        }



    }


    public void processJoin2TempTablesQuery(JavaSparkContext ctx) {

        SQLJobContext sqlJobContext = new SQLJobContext(ctx,
                new ConnectionBean(CUSTOMER_TABLE, REDSHIFT_DRIVER_JAR, REDSHIFT_JDBC41_DRIVER, REDSHIFT_CONNECTION_URL));
        SQLContext sqlContext = sqlJobContext.getSqlContext();
        sqlJobContext.registerTable(CUSTOMER_TABLE,"customer");
        sqlJobContext.registerTable(SUPPLIER_TABLE,"supplier");


        DataFrame dataFrame = sqlContext.sql(SELECT_JOIN_10);
        List<Row> dataList = dataFrame.collectAsList();
        for (Row row : dataList) {
            System.out.println("Cust: "+ row.getString(0)+" -SUP" +row.getString(1));
        }



    }

}
