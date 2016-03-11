package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class XmlParserReducerTest {
	private ReduceDriver<IntWritable, ArticleInfo, AvroKey<Integer>, AvroValue<ArticleInfo>> reduceDriver;

	@Before
	public void setup() {
		reduceDriver = ReduceDriver.newReduceDriver(new XmlParserReducer());
		Configuration driverConfiguration = reduceDriver.getConfiguration();

		String[] ioSerializations = driverConfiguration.getStrings("io.serializations");
		String[] newIOSerializations = new String[ioSerializations.length + 1];
		System.arraycopy(ioSerializations, 0, newIOSerializations, 0, ioSerializations.length);
		newIOSerializations[newIOSerializations.length - 1] = AvroSerialization.class.getName();

		driverConfiguration.setStrings("io.serializations", newIOSerializations);
		driverConfiguration.set("avro.serialization.value.writer.schema", XmlParserJob.SCHEMA.toString(true));
		driverConfiguration.set("avro.serialization.key.writer.schema", Schema.create(Schema.Type.INT).toString(true));
	}

	@Test
	public void testReducer() throws IOException {
		Calendar calendar = Calendar.getInstance();
		calendar.set(2011, 9 - 1, 28, 0, 0, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		ArticleInfo articleInfo = new ArticleInfo(
				"/user/ubuntu/datasets/pubmed/unzipped/unzipped.A-B/3_Biotech/3_Biotech_2011_Dec_13_1(4)_217-225.nxml",
				"Evaluation of indigenous Trichoderma isolates from Manipur as biocontrol agent against Pythium aphanidermatum on common beans",
				27L, "2190-572X", calendar.getTimeInMillis());
		reduceDriver.withInput(new IntWritable(1), Arrays.asList(articleInfo))
				.withOutput(new AvroKey<Integer>(1), new AvroValue<ArticleInfo>(articleInfo)).runTest();
	}
}
