package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.Repetition;

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

	private MapDriver<AvroKey<GenericRecord>, NullWritable, NullWritable, AVROToParquetArrayWritable> mapDriver;
	private GenericRecord data1;
	private GenericRecord data2;
	private AVROToParquetArrayWritable expectedData1;
	private AVROToParquetArrayWritable expectedData2;

	@Before
	public void setUp() throws IOException {
		mapDriver = MapDriver.newMapDriver(new AVROInputMapper());

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

		MessageType parquetSchema = createParquetSchema();

		Writable[] writableData1 = new Writable[5];
		writableData1[0] = new Text((String) data1.get(FILE_PATH));
		writableData1[1] = new Text((String) data1.get(ARTICLE_TITLE));
		writableData1[2] = new LongWritable((Long) data1.get(ARTICLE_PUBLISHER_ID));
		writableData1[3] = new Text((String) data1.get(ARTICLE_ISSN_P_PUB));
		writableData1[4] = new LongWritable((Long) data1.get(ARTICLE_DATE_ACCEPTED));
		expectedData1 = new AVROToParquetArrayWritable(writableData1, parquetSchema.toString());

		Writable[] writableData2 = new Writable[5];
		writableData2[0] = new Text((String) data2.get(FILE_PATH));
		writableData2[1] = new Text((String) data2.get(ARTICLE_TITLE));
		writableData2[2] = new LongWritable((Long) data2.get(ARTICLE_PUBLISHER_ID));
		writableData2[3] = new Text((String) data2.get(ARTICLE_ISSN_P_PUB));
		writableData2[4] = new LongWritable((Long) data2.get(ARTICLE_DATE_ACCEPTED));
		expectedData2 = new AVROToParquetArrayWritable(writableData2, parquetSchema.toString());
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new AvroKey<GenericRecord>(data1), NullWritable.get());
		mapDriver.withInput(new AvroKey<GenericRecord>(data2), NullWritable.get());

		mapDriver.withOutput(NullWritable.get(), expectedData1);
		mapDriver.withOutput(NullWritable.get(), expectedData2);

		mapDriver.runTest();
	}

	private MessageType createParquetSchema() {
		parquet.schema.Type[] types = new parquet.schema.Type[SCHEMA.getFields().size()];
		for (Field field : SCHEMA.getFields()) {
			parquet.schema.Type type = getType(field);
			if (null != type) {
				types[field.pos()] = type;
			}
		}
		return new MessageType("pubmed", types);
	}

	private PrimitiveType getType(Field field) {
		PrimitiveType type = null;

		if (null != field && null != field.schema()) {
			Schema fieldSchema = field.schema();
			type = new PrimitiveType(Repetition.REQUIRED, getParquetRecordTypeFromAVROType(fieldSchema.getType()),
					field.name());
		}

		return type;
	}

	private PrimitiveTypeName getParquetRecordTypeFromAVROType(Type type) {
		PrimitiveTypeName name = PrimitiveTypeName.BINARY;
		switch (type) {
		case STRING:
			name = PrimitiveTypeName.BINARY;
			break;
		case LONG:
		case INT:
			name = PrimitiveTypeName.INT64;
			break;

		default:
			name = PrimitiveTypeName.BINARY;
			break;
		}
		return name;
	}
}
