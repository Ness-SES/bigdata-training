package com.ness.spark.jobs.sql.config;

public class ConnectionBean {
    private final String driverJar;
    private final String sqlDriver;
    private final String connectionUrl;
    private String dbTable;

    public ConnectionBean(String dbTable,String driverJar, String sqlDriver, String connectionUrl) {
        this.dbTable = dbTable;
        this.driverJar = driverJar;
        this.sqlDriver = sqlDriver;
        this.connectionUrl = connectionUrl;
    }

    public String getDriverJar() {
        return driverJar;
    }

    public String getSqlDriver() {
        return sqlDriver;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public String getDBTable() {
        return dbTable;
    }
}
