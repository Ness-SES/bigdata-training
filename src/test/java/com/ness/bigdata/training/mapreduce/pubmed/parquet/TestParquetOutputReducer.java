package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.Repetition;

public class TestParquetOutputReducer {

	private static final String FILE_PATH = "filePath";
	private static final String ARTICLE_TITLE = "articleTitle";
	private static final String ARTICLE_PUBLISHER_ID = "articlePublisherId";
	private static final String ARTICLE_ISSN_P_PUB = "articleIssnPPub";
	private static final String ARTICLE_DATE_ACCEPTED = "articleDateAccepted";

	private ReduceDriver<NullWritable, AVROToParquetArrayWritable, Void, ArrayWritable> reduceDriver;
	private ArrayWritable expectedData1;
	private ArrayWritable expectedData2;
	private MessageType parquetSchema;

	@Before
	public void setUp() throws IOException {
		reduceDriver = ReduceDriver.newReduceDriver(new ParquetOutputReducer());

		parquetSchema = createParquetSchema();

		Writable[] writableData1 = new Writable[5];
		writableData1[0] = new Text("FP" + 1);
		writableData1[1] = new Text("AT" + 1);
		writableData1[2] = new LongWritable(Long.valueOf(1));
		writableData1[3] = new Text("AIPP" + 1);
		writableData1[4] = new LongWritable(Long.valueOf((System.currentTimeMillis() - 1) / 1000L));
		expectedData1 = new ArrayWritable(Writable.class, writableData1);

		Writable[] writableData2 = new Writable[5];
		writableData2[0] = new Text("FP" + 2);
		writableData2[1] = new Text("AT" + 2);
		writableData2[2] = new LongWritable(Long.valueOf(2));
		writableData2[3] = new Text("AIPP" + 2);
		writableData2[4] = new LongWritable(Long.valueOf((System.currentTimeMillis() - 2) / 1000L));
		expectedData2 = new ArrayWritable(Writable.class, writableData2);
	}

	@Test
	public void testReducer() throws IOException {
		List<AVROToParquetArrayWritable> values = new ArrayList<AVROToParquetArrayWritable>();
		values.add(new AVROToParquetArrayWritable(expectedData1.get(), parquetSchema.toString()));
		values.add(new AVROToParquetArrayWritable(expectedData2.get(), parquetSchema.toString()));

		reduceDriver.withInput(NullWritable.get(), values);

		reduceDriver.withOutput(null, expectedData1);
		reduceDriver.withOutput(null, expectedData2);

		reduceDriver.runTest();
	}

	private MessageType createParquetSchema() {
		parquet.schema.Type[] types = new parquet.schema.Type[5];
		types[0] = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, FILE_PATH);
		types[1] = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, ARTICLE_TITLE);
		types[2] = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, ARTICLE_PUBLISHER_ID);
		types[3] = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, ARTICLE_ISSN_P_PUB);
		types[4] = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, ARTICLE_DATE_ACCEPTED);

		return new MessageType("pubmed", types);
	}
}
