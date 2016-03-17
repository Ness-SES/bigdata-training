package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.writable.BinaryWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import parquet.avro.AvroSchemaConverter;
import parquet.io.api.Binary;
import parquet.schema.MessageType;

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

	private MapDriver<AvroKey<GenericRecord>, NullWritable, Void, ArrayWritable> mapDriver;
	private GenericRecord data1;
	private GenericRecord data2;
	private AvroToParquetArrayWritable expectedData1;
	private AvroToParquetArrayWritable expectedData2;

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
		data1.put(ARTICLE_PUBLISHER_ID, Long.valueOf(1));
		data1.put(ARTICLE_ISSN_P_PUB, "AIPP" + 1);
		data1.put(ARTICLE_DATE_ACCEPTED, Long.valueOf((System.currentTimeMillis() - 1) / 1000L));

		data2 = new GenericData.Record(SCHEMA);
		data2.put(FILE_PATH, "FP" + 1);
		data2.put(ARTICLE_TITLE, "AT" + 1);
		data2.put(ARTICLE_PUBLISHER_ID, Long.valueOf(1));
		data2.put(ARTICLE_ISSN_P_PUB, "AIPP" + 1);
		data2.put(ARTICLE_DATE_ACCEPTED, Long.valueOf((System.currentTimeMillis() - 1) / 1000L));

		MessageType parquetSchema = new AvroSchemaConverter().convert(SCHEMA);

		Writable[] writableData1 = new Writable[5];
		writableData1[0] = new BinaryWritable(Binary.fromString((String) data1.get(FILE_PATH)));
		writableData1[1] = new BinaryWritable(Binary.fromString((String) data1.get(ARTICLE_TITLE)));
		writableData1[2] = new LongWritable((Long) data1.get(ARTICLE_PUBLISHER_ID));
		writableData1[3] = new BinaryWritable(Binary.fromString((String) data1.get(ARTICLE_ISSN_P_PUB)));
		writableData1[4] = new LongWritable((Long) data1.get(ARTICLE_DATE_ACCEPTED));
		expectedData1 = new AvroToParquetArrayWritable(writableData1, parquetSchema.toString());

		Writable[] writableData2 = new Writable[5];
		writableData2[0] = new BinaryWritable(Binary.fromString((String) data2.get(FILE_PATH)));
		writableData2[1] = new BinaryWritable(Binary.fromString((String) data2.get(ARTICLE_TITLE)));
		writableData2[2] = new LongWritable((Long) data2.get(ARTICLE_PUBLISHER_ID));
		writableData2[3] = new BinaryWritable(Binary.fromString((String) data2.get(ARTICLE_ISSN_P_PUB)));
		writableData2[4] = new LongWritable((Long) data2.get(ARTICLE_DATE_ACCEPTED));
		expectedData2 = new AvroToParquetArrayWritable(writableData2, parquetSchema.toString());
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new AvroKey<GenericRecord>(data1), NullWritable.get());
		mapDriver.withInput(new AvroKey<GenericRecord>(data2), NullWritable.get());

		// mapDriver.withOutput(NullWritable.get(), expectedData1);
		// mapDriver.withOutput(NullWritable.get(), expectedData2);

		mapDriver.runTest();
	}
}
