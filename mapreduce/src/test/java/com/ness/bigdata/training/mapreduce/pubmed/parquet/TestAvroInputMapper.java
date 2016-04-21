package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class TestAvroInputMapper {

    private static final Schema SCHEMA = new Schema.Parser().parse("{" + "\"type\": \"record\","
            + "\"name\": \"ArticleInfo\"," + "\"fields\": [" + "	{\"name\": \"filePath\", \"type\": \"string\"},"
            + "	{\"name\": \"articleTitle\", \"type\": \"string\"},"
            + "	{\"name\": \"articlePublisherId\", \"type\": \"long\"},"
            + "	{\"name\": \"articleIssnPPub\", \"type\": \"string\"},"
            + "	{\"name\": \"articleDateAccepted\", \"type\": \"long\"}" + "]" + "}");
    private static final String FILE_PATH = "filePath";
    private static final String ARTICLE_TITLE = "articleTitle";
    private static final String ARTICLE_PUBLISHER_ID = "articlePublisherId";
    private static final String ARTICLE_ISSN_P_PUB = "articleIssnPPub";
    private static final String ARTICLE_DATE_ACCEPTED = "articleDateAccepted";

    private MapDriver<AvroKey<GenericRecord>, NullWritable, NullWritable, ArrayWritable> mapDriver;
    private GenericRecord data1;
    private GenericRecord data2;
    private Writable[] writableData1;
    private Writable[] writableData2;

    @Before
    public void setUp() throws IOException {
        mapDriver = MapDriver.newMapDriver(new AvroInputMapper());

        Configuration driverConfiguration = mapDriver.getConfiguration();
        // driverConfiguration.set(Constants.SCHEMA_KEY, SCHEMA.toString());

        String[] ioSerializations = driverConfiguration.getStrings("io.serializations");
        String[] newIOSerializations = new String[ioSerializations.length + 1];
        System.arraycopy(ioSerializations, 0, newIOSerializations, 0, ioSerializations.length);
        newIOSerializations[newIOSerializations.length - 1] = AvroSerialization.class.getName();

        driverConfiguration.setStrings("io.serializations", newIOSerializations);
        driverConfiguration.set("avro.serialization.key.reader.schema", SCHEMA.toString(true));
        driverConfiguration.set("avro.serialization.key.writer.schema", SCHEMA.toString(true));

        data1 = new GenericData.Record(SCHEMA);
        data1.put(FILE_PATH, "FP" + 1);
        data1.put(ARTICLE_TITLE, "AT" + 1);
        data1.put(ARTICLE_PUBLISHER_ID, 1L);
        data1.put(ARTICLE_ISSN_P_PUB, "AIPP" + 1);
        data1.put(ARTICLE_DATE_ACCEPTED, (System.currentTimeMillis() - 1) / 1000L);

        data2 = new GenericData.Record(SCHEMA);
        data2.put(FILE_PATH, "FP" + 1);
        data2.put(ARTICLE_TITLE, "AT" + 1);
        data2.put(ARTICLE_PUBLISHER_ID, 1L);
        data2.put(ARTICLE_ISSN_P_PUB, "AIPP" + 1);
        data2.put(ARTICLE_DATE_ACCEPTED, (System.currentTimeMillis() - 1) / 1000L);

        writableData1 = new Writable[5];
        writableData1[0] = new BytesWritable(data1.get(FILE_PATH).toString().getBytes());
        writableData1[1] = new BytesWritable(data1.get(ARTICLE_TITLE).toString().getBytes());
        writableData1[2] = new LongWritable((Long) data1.get(ARTICLE_PUBLISHER_ID));
        writableData1[3] = new BytesWritable(data1.get(ARTICLE_ISSN_P_PUB).toString().getBytes());
        writableData1[4] = new LongWritable((Long) data1.get(ARTICLE_DATE_ACCEPTED));

        writableData2 = new Writable[5];
        writableData2[0] = new BytesWritable(data2.get(FILE_PATH).toString().getBytes());
        writableData2[1] = new BytesWritable(data2.get(ARTICLE_TITLE).toString().getBytes());
        writableData2[2] = new LongWritable((Long) data2.get(ARTICLE_PUBLISHER_ID));
        writableData2[3] = new BytesWritable(data2.get(ARTICLE_ISSN_P_PUB).toString().getBytes());
        writableData2[4] = new LongWritable((Long) data2.get(ARTICLE_DATE_ACCEPTED));
    }

    @Test
    @Ignore
    public void testMapper() throws IOException {
        mapDriver.withInput(new AvroKey<>(data1), NullWritable.get());
        mapDriver.withInput(new AvroKey<>(data2), NullWritable.get());

        mapDriver.withOutput(NullWritable.get(), new ArrayWritable(Writable.class, writableData1));
        mapDriver.withOutput(NullWritable.get(), new ArrayWritable(Writable.class, writableData2));

        mapDriver.runTest();
    }
}
