package com.ness.spark.jobs;

import org.apache.log4j.Logger;
import java.sql.*;
import java.util.Properties;

/**
 * Created by p3700622 on 30/03/16.
 */
public class JdbcQueryExecutor {

    private String url;
    private String userName;
    private String password;
    private  Logger log = Logger.getLogger(JdbcQueryExecutor.class);
    private  String driverName ;

    public JdbcQueryExecutor(String driverName, String url, String userName, String password) {
        this.driverName = driverName;
        this.url = url;
        this.userName = userName;
        this.password = password;

    }

    public void setLog(Logger log) {
        this.log = log;
    }

    public ResultSet executeQuery(String sql) {
        ResultSet resultSet = null;
        Connection connection = null;
        Statement stmt = null;
        try {
            Class.forName(driverName);
            connection = getConnection(url);
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery(sql);
            log.info("results number" + resultSet.getFetchSize());
            log.info("first result " + resultSet.getObject(1).toString() +" : "+resultSet.getObject(2).toString());

        } catch (SQLException e) {
            log.error(e.getMessage());
            e.printStackTrace();

        } catch (ClassNotFoundException e) {
            log.error(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                stmt.close();
                connection.close();
            } catch (SQLException e) {
                log.error(e.getMessage());
            }
        }
        return resultSet;
    }

    public void executeStatement(String sql) {
        Connection connection = null;
        Statement stmt = null;
        try {
            Class.forName(driverName);
            connection = getConnection(url);
            stmt = connection.createStatement();
            stmt.executeUpdate(sql);

        } catch (SQLException e) {
            log.error(e.getMessage());
            e.printStackTrace();

        } catch (ClassNotFoundException e) {
            log.error(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                stmt.close();
                connection.close();
            } catch (SQLException e) {
                log.error(e.getMessage());
            }
        }

    }


    public Connection getConnection(String url) throws SQLException {

        Connection conn = null;
        Properties connectionProps = new Properties();
        connectionProps.put("user", this.userName);
        connectionProps.put("password", this.password);

        conn = DriverManager.getConnection(url, connectionProps);

        log.info("Connected to database");
        return conn;
    }


    public void setUserName(String userName) {
        this.userName = userName;
    }


}