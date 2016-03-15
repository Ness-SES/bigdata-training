package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.Repetition;

public class AVROInputMapper
		extends Mapper<AvroKey<GenericRecord>, NullWritable, NullWritable, AVROToParquetArrayWritable> {

	private static Schema schema;
	private static MessageType parquetSchema;
	private static AVROToParquetArrayWritable resultedData;
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
		if (null == schema) {
			schema = data.getSchema();
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
			switch (field.schema().getType()) {
			case STRING:
				String strValue = (String) data.get(field.name());
				if (null != strValue) {
					resultedDataArray[field.pos()] = new Text((String) data.get(field.name()));
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
		} else if (null == resultedData) {
			dataInstantiated = false;
		}
		return dataInstantiated;
	}

	private void createParquetSchema(Configuration configuration) {
		if (null == schema || null == schema.getFields() || schema.getFields().isEmpty()) {
			return;
		}
		parquet.schema.Type[] types = new parquet.schema.Type[schema.getFields().size()];
		for (Field field : schema.getFields()) {
			parquet.schema.Type type = getType(field);
			if (null != type) {
				types[field.pos()] = type;
			}
		}
		resultedData = new AVROToParquetArrayWritable(Writable.class);
		resultedDataArray = new Writable[types.length];
		parquetSchema = new MessageType("pubmed", types);
		DataWritableWriteSupport.setSchema(parquetSchema, configuration);
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
