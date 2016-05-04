package com.ness.spark.jobs.sql;

import com.ness.spark.jobs.sql.config.ConnectionBean;
import com.ness.spark.jobs.sql.config.QueryJobConstants;
import com.ness.spark.jobs.sql.config.SQLJobContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;

/**
 * Created by p3700622 on 01/04/16.
 */
public class UDFQueryJob extends KryoSerializer {

    public UDFQueryJob(SparkConf conf) {
        super(conf);
    }

    public void processJoinTableQuery(JavaSparkContext ctx) {

        SQLJobContext sqlJobContext = new SQLJobContext(ctx, new ConnectionBean(null, QueryJobConstants.REDSHIFT_DRIVER_JAR, QueryJobConstants.REDSHIFT_JDBC41_DRIVER, QueryJobConstants.REDSHIFT_CONNECTION_URL));
        SQLContext sqlContext = sqlJobContext.getSqlContext();
        sqlJobContext.registerTable(QueryJobConstants.CUSTOMER_TABLE,"customers");

        sqlContext.udf().register("custNameUDF", new UDF1<String, String>() {

            public String call(String arg) throws Exception {
                String key = "1-" + arg;
                return key;
            }
        }, DataTypes.StringType);


        DataFrame dataFrame = sqlContext.sql("SELECT custNameUDF(c_name) from customers  limit 10");//SELECT custNameUDF(c_name) from customer
        List<Row> customerList = dataFrame.collectAsList();
        for (Row row : customerList) {
            System.out.println("Customer: " + row.getString(0));
        }

    }


}
