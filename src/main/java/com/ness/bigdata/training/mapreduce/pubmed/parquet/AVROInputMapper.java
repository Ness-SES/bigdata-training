package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.IOException;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.ness.bigdata.training.mapreduce.pubmed.ArticleInfo;

public class AVROInputMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, ArticleInfo, NullWritable> {

	private static ArticleInfo object = new ArticleInfo();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	}

	@Override
	protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		boolean dataInstantiated = instantiateData(key.datum());
		if (true == dataInstantiated) {
			context.write(object, NullWritable.get());
		}
	}

	private boolean instantiateData(GenericRecord data) {
		if (null == data || null == data.getSchema() || null == data.getSchema().getFields()
				|| data.getSchema().getFields().isEmpty()) {
			return false;
		}
		boolean dataInstantiated = false;
		for (Field field : data.getSchema().getFields()) {
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
}
