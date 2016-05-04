package com.ness.spark.jobs.sql.config;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Map;

import static com.ness.spark.jobs.sql.config.QueryJobConstants.CLASSES_JAR;

/**
 * Created by p3700622 on 30/03/16.
 */
public class SQLJobContext {

    private JavaSparkContext ctx;
    private ConnectionBean connectionBean;
    private SQLContext sqlContext;

    public SQLJobContext(JavaSparkContext ctx, ConnectionBean connectionBean) {
        this.ctx = ctx;
        this.connectionBean = connectionBean;
        initContext();
    }


    private void initContext() {
        ctx.addJar(connectionBean.getDriverJar());
        ctx.addJar(CLASSES_JAR);
        sqlContext = new SQLContext(ctx);
        Map<String, String> options = getSQLOptionsMap(connectionBean.getDBTable(),connectionBean.getSqlDriver(),connectionBean.getConnectionUrl());
        sqlContext.read().format("jdbc").options(options);

    }

    public void registerTable(String dbTable, String registeredTableName) {
        Map<String, String> options = getSQLOptionsMap(dbTable,connectionBean.getSqlDriver(),connectionBean.getConnectionUrl());
        sqlContext.read().format("jdbc").options(options).load().registerTempTable(registeredTableName);
    }

    private Map<String, String> getSQLOptionsMap(String dbTable, String sqlDriver, String connectionUrl) {
        //Datasource options
        Map<String, String> options = new HashMap<String,String>();
        options.put("driver", sqlDriver);
        options.put("url", connectionUrl);
        if (dbTable != null)
            options.put("dbtable", dbTable);
        return options;
    }

    public SQLContext getSqlContext() {
        return sqlContext;
    }

}
