package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.writable.BinaryWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import parquet.avro.AvroSchemaConverter;
import parquet.io.api.Binary;
import parquet.schema.MessageType;

public class AvroInputMapper
		extends Mapper<AvroKey<GenericRecord>, NullWritable, NullWritable, AvroToParquetArrayWritable> {

	private static Schema schema;
	private static MessageType parquetSchema;
	private static AvroToParquetArrayWritable resultedData;
	private static Writable[] resultedDataArray;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	}

	@Override
	protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		initializeSchemas(key.datum(), context.getConfiguration());
		if (true == instantiateData(key.datum(), context.getConfiguration())) {
			context.write(NullWritable.get(), resultedData);
		}
	}

	private void initializeSchemas(GenericRecord data, Configuration configuration) {
		if (null == data || null == data.getSchema() || null == configuration) {
			return;
		}

		schema = data.getSchema();
		createParquetSchema();
	}

	private boolean instantiateData(GenericRecord data, Configuration configuration) {
		if (null == data || null == data.getSchema() || null == data.getSchema().getFields()
				|| data.getSchema().getFields().isEmpty() || null == configuration) {
			return false;
		}
		boolean dataInstantiated = false;
		for (Field field : schema.getFields()) {
			switch (field.schema().getType()) {
			case STRING:
				String strValue = (String) data.get(field.name());
				if (null != strValue) {
					resultedDataArray[field.pos()] = new BinaryWritable(
							Binary.fromString((String) data.get(field.name())));
				}
				break;
			case LONG:
				Long longValue = (Long) data.get(field.name());
				if (null != longValue) {
					resultedDataArray[field.pos()] = new LongWritable((Long) data.get(field.name()));
				}
				break;
			case INT:
				Integer intValue = (Integer) data.get(field.name());
				if (null != intValue) {
					resultedDataArray[field.pos()] = new IntWritable((Integer) data.get(field.name()));
				}
				break;
			default:
				break;
			}
			dataInstantiated = true;
		}
		if (true == dataInstantiated && null != resultedData) {
			resultedData.set(resultedDataArray);
			resultedData.setSchema(parquetSchema.toString());
		} else if (null == resultedData) {
			dataInstantiated = false;
		}
		return dataInstantiated;
	}

	private void createParquetSchema() {
		if (null == schema || null == schema.getFields() || schema.getFields().isEmpty()) {
			return;
		}
		resultedData = new AvroToParquetArrayWritable();
		resultedDataArray = new Writable[schema.getFields().size()];
		parquetSchema = new AvroSchemaConverter().convert(schema);
	}
}
