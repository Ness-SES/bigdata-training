package com.ness.spark.jobs.json;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created by p3700622 on 07/04/16.
 */
public class JsonUtils implements Serializable {
    transient JavaSparkContext ctx;
    private static final Pattern SPACE = Pattern.compile(" ");
    private SQLContext sqlContext;

    public JsonUtils(JavaSparkContext ctx) {
        this.ctx = ctx;
    }

    public DataFrame getJsonRecords( String path) {
        JavaRDD<String> lines = ctx.textFile(path, 3);

        JavaRDD<String> jsonString = getJsonStringJavaRDD(lines);

        sqlContext = new SQLContext(ctx);

        DataFrame dataFrame = sqlContext.read().json(jsonString);
        registerTempTable(dataFrame, "jsonTable");

        return dataFrame;

    }
    public DataFrame getJsonRecords(String path, HiveContext hiveContext) {
        JavaRDD<String> lines = ctx.textFile(path, 3);

        JavaRDD<String> jsonString = getJsonStringJavaRDD(lines);

        HiveContext sqlContext = new HiveContext(ctx);

        DataFrame dataFrame = sqlContext.read().json(jsonString);
        registerTempTable(dataFrame, "jsonTable");

        return dataFrame;

    }

    public void registerTempTable(DataFrame df, String tableName) {
        df.registerTempTable(tableName);
    }

    public JavaRDD<String> getJsonStringJavaRDD(JavaRDD<String> lines) {
        return lines.map( new Function<String, String>() {

            public String call(String line) {
                String[] parts = line.split("\t");

                return parts[1];
            }
        });
    }

    public DataFrame selectSqlContext(String sqlString, String tableName) {

        DataFrame dataFrame = sqlContext.sql(sqlString);
        registerTempTable(dataFrame,tableName);
        return dataFrame;
    }
    public Integer selectCountDataFrame(DataFrame df, String selectItem) {
        int result = df.select(selectItem).collect().length;
        return result;
    }

    public double selectSumDataFrame(DataFrame df, String selectItem) {
        df.show();
        DataFrame count = df.groupBy(selectItem).count();
        count.show();

        return count.collect().length;
    }



    public JavaRDD<String> splitToRDD(final Pattern pattern, String path) {
        JavaRDD<String> lines = ctx.textFile(path, 3);
        System.out.println(lines.collect().get(0).split("\t")[0].split(":")[0]);

        JavaRDD<String> records = lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterable<String> call(String s) {
                return Arrays.asList(pattern.split(s));
            }
        });
        return records;
    }

    public void splitToRDD(String jsonFile) {
        splitToRDD(SPACE, jsonFile).count();
    }




    public JavaRDD<JsonRecord> getJsonRecordJavaRDD(JavaRDD<String> lines) {
        return lines.map( new Function<String, JsonRecord>() {

            public JsonRecord call(String line) {
                String[] parts = line.split("\t");

                JsonRecord record = new JsonRecord(parts[0],parts[1]);

                return record;
            }
        });
    }

    class JsonRecord implements Serializable{
        private final String header;
        private final String jsonRecord;

        JsonRecord(String header, String jsonRecord){

            this.header = header;
            this.jsonRecord = jsonRecord;
        }
    }

}