package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class XmlParserReducerTest {
    private ReduceDriver<IntWritable, MapWritable, AvroKey<Integer>, AvroValue<GenericRecord>> reduceDriver;

    @Before
    public void setup() {
        reduceDriver = ReduceDriver.newReduceDriver(new XmlParserReducer());
        Configuration driverConfiguration = reduceDriver.getConfiguration();

        String[] ioSerializations = driverConfiguration.getStrings("io.serializations");
        String[] newIOSerializations = new String[ioSerializations.length + 1];
        System.arraycopy(ioSerializations, 0, newIOSerializations, 0, ioSerializations.length);
        newIOSerializations[newIOSerializations.length - 1] = AvroSerialization.class.getName();

        driverConfiguration.setStrings("io.serializations", newIOSerializations);
        driverConfiguration.set("avro.serialization.value.writer.schema", TestData.AVRO_SCHEMA.toString(true));
        driverConfiguration.set("avro.serialization.key.writer.schema", Schema.create(Schema.Type.INT).toString(true));
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new IntWritable(1), Arrays.asList(TestData.ARTICLE_INFO_MAP))
                .withOutput(new AvroKey<Integer>(1), new AvroValue<GenericRecord>(TestData.ARTICLE_INFO_GENERIC_RECORD))
                .runTest();
    }
}
