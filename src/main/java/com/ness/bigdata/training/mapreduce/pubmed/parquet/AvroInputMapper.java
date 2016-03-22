package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.writable.BinaryWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import parquet.io.api.Binary;

public class AvroInputMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Void, ArrayWritable> {

	private static Schema schema;
	private static Writable[] resultedDataArray;

	@Override
	protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		initializeSchemas(context.getConfiguration());
		if (true == instantiateData(key.datum(), context.getConfiguration())) {
			context.write(null, new ArrayWritable(Writable.class, resultedDataArray));
		}
	}

	private void initializeSchemas(Configuration configuration) {
		if (null == schema) {
			String strSchema = configuration.get("parquet.from.avro.schema");
			schema = new Schema.Parser().parse(strSchema);
			createParquetSchema(configuration);
		}
	}

	private boolean instantiateData(GenericRecord data, Configuration configuration) {
		if (null == data || null == data.getSchema() || null == data.getSchema().getFields()
				|| data.getSchema().getFields().isEmpty() || null == configuration) {
			return false;
		}
		boolean dataInstantiated = false;
		for (Field field : schema.getFields()) {
			Object value = data.get(field.name());
			switch (field.schema().getType()) {
			case STRING:
				if (null != value) {
					resultedDataArray[field.pos()] = new BinaryWritable(Binary.fromString((String) value));
				}
				break;
			case LONG:
				if (null != value) {
					resultedDataArray[field.pos()] = new LongWritable((Long) value);
				}
				break;
			case INT:
				if (null != value) {
					resultedDataArray[field.pos()] = new IntWritable((Integer) value);
				}
				break;
			default:
				resultedDataArray[field.pos()] = null;
				break;
			}

			dataInstantiated = true;
		}
		return dataInstantiated;
	}

	private void createParquetSchema(Configuration config) {
		if (null == schema || null == schema.getFields() || schema.getFields().isEmpty()) {
			return;
		}
		resultedDataArray = new Writable[schema.getFields().size()];
	}
}
