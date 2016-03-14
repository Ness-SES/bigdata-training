package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.ness.bigdata.training.mapreduce.pubmed.ArticleInfo;

public class TestAVROInputMapper {

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

	private MapDriver<AvroKey<GenericRecord>, NullWritable, ArticleInfo, IntWritable> mapDriver;
	private GenericRecord data1;
	private GenericRecord data2;
	private ArticleInfo expectedData1;
	private ArticleInfo expectedData2;

	@Before
	public void setUp() throws IOException {
		mapDriver = MapDriver.newMapDriver(new AVROInputMapper());

		Configuration driverConfiguration = mapDriver.getConfiguration();
		driverConfiguration.set(Constants.SCHEMA_KEY, SCHEMA.toString());

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
		data1.put(ARTICLE_PUBLISHER_ID, Long.valueOf(1));
		data1.put(ARTICLE_ISSN_P_PUB, "AIPP" + 1);
		data1.put(ARTICLE_DATE_ACCEPTED, Long.valueOf((System.currentTimeMillis() - 1) / 1000L));

		data2 = new GenericData.Record(SCHEMA);
		data2.put(FILE_PATH, "FP" + 1);
		data2.put(ARTICLE_TITLE, "AT" + 1);
		data2.put(ARTICLE_PUBLISHER_ID, Long.valueOf(1));
		data2.put(ARTICLE_ISSN_P_PUB, "AIPP" + 1);
		data2.put(ARTICLE_DATE_ACCEPTED, Long.valueOf((System.currentTimeMillis() - 1) / 1000L));

		expectedData1 = new ArticleInfo((String) data1.get(FILE_PATH), (String) data1.get(ARTICLE_TITLE),
				(Long) data1.get(ARTICLE_PUBLISHER_ID), (String) data1.get(ARTICLE_ISSN_P_PUB),
				(Long) data1.get(ARTICLE_DATE_ACCEPTED));
		expectedData2 = new ArticleInfo((String) data2.get(FILE_PATH), (String) data2.get(ARTICLE_TITLE),
				(Long) data2.get(ARTICLE_PUBLISHER_ID), (String) data2.get(ARTICLE_ISSN_P_PUB),
				(Long) data2.get(ARTICLE_DATE_ACCEPTED));
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new AvroKey<GenericRecord>(data1), NullWritable.get());
		mapDriver.withInput(new AvroKey<GenericRecord>(data2), NullWritable.get());

		mapDriver.withOutput(expectedData1, new IntWritable(1));
		mapDriver.withOutput(expectedData2, new IntWritable(1));

		mapDriver.runTest();
	}
}
