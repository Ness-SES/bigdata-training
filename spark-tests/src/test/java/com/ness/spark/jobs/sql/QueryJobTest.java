package com.ness.spark.jobs.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.*;

/**
 * Created by p3700622 on 19/04/16.
 */

/**
 * Configurations are for a local  spark, training purpose
 */
public class QueryJobTest {
    public static final String APP_NAME = "JavaSparkSQL";
    public static final String SPARK_MASTER_URL = "spark://127.0.0.1:7077";
    static SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);
    static JavaSparkContext sc;



    @BeforeClass
    public static void init() {
        SparkConf conf = sparkConf.setMaster(SPARK_MASTER_URL);
        sc = new JavaSparkContext(conf);

    }

    @Before
    public void setUp() throws Exception {

    }

    @AfterClass
    public static void tearDown() throws Exception {

        sc.close();
    }

    @Test
    public  void testJoin2Tables() throws Exception {
         new JoinQueryJob().processJoin2TempTablesQuery(sc);
    }
    @Ignore
    public  void testJoinTables() throws Exception {
        new JoinQueryJob().processJoinTableQuery(sc);
    }

    @Test
    public  void testSelectCountTables() throws Exception {
        new SelectQueryJob(sparkConf).processSelectCountQuery(sc);

    }
    @Test
    public  void testSelectCountFuncTables() throws Exception {
        new SelectQueryJob(sparkConf).processSelectCountFUNCQuery(sc);

    }
    @Test
    public  void testSelectFuncTables() throws Exception {
        new SelectQueryJob(sparkConf).processSelectFUNCQuery(sc);

    }
    @Test
    public  void testSelectQueryTables() throws Exception {
        new SelectQueryJob(sparkConf).processSelectQuery(sc);

    }

    @Test
    public  void testUDFTables() throws Exception {
         new UDFQueryJob(sparkConf).processJoinTableQuery(sc);

    }

}