package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.ness.bigdata.training.mapreduce.pubmed.ArticleInfo;

import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.Repetition;

public class AVROInputMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, ArticleInfo, NullWritable> {

	private static ArticleInfo object = new ArticleInfo();
	private static Schema schema;
	private static MessageType parquetSchema;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	}

	@Override
	protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		initializeSchemas(key.datum(), context.getConfiguration());
		boolean dataInstantiated = instantiateData(key.datum(), context.getConfiguration());
		if (true == dataInstantiated) {
			context.write(object, NullWritable.get());
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
			Object value = data.get(field.name());
			if (null != value) {
				setValue(object, field.name(), value);
			} else {
				setValue(object, field.name(), field.defaultValue());
			}
			dataInstantiated = true;
		}
		return dataInstantiated;
	}

	private void setValue(ArticleInfo object, String field, Object value) {
		Class<?> clazz = object.getClass();
		try {
			java.lang.reflect.Field objField = clazz.getDeclaredField(field);
			objField.setAccessible(true);
			objField.set(object, value);
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
		}
	}

	private void createParquetSchema(Configuration configuration) {
		if (null == schema) {
			return;
		}
		List<parquet.schema.Type> types = new LinkedList<parquet.schema.Type>();
		for (Field field : schema.getFields()) {
			parquet.schema.Type type = getType(field);
			if (null != type) {
				types.add(type);
			}
		}
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
