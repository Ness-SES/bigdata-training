package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.ness.bigdata.training.mapreduce.pubmed.ArticleInfo;

public class AVROInputMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, ArticleInfo, NullWritable> {

	private static Schema schema = null;
	private static List<String> schemaFields;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		loadSchema(context);
	}

	@Override
	protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		if (null == schema || null == schemaFields || schemaFields.isEmpty()) {
			return;
		}
		ArticleInfo info = instantiateData(key);
		context.write(info, NullWritable.get());
	}

	private void loadSchema(Context context) {
		if (null == schema && null != context) {
			Configuration config = context.getConfiguration();
			if (null == config) {
				return;
			}
			String strSchema = config.get(Constants.SCHEMA_KEY);
			if (null == strSchema) {
				return;
			}
			schema = new Schema.Parser().parse(strSchema);
			loadFieldsList();
		}
	}

	private void loadFieldsList() {
		if (null == schemaFields) {
			schemaFields = new LinkedList<String>();
		}
		for (Field field : schema.getFields()) {
			schemaFields.add(field.name());
		}
	}

	private ArticleInfo instantiateData(AvroKey<GenericRecord> key) {
		if (null == schemaFields || schemaFields.isEmpty()) {
			return null;
		}
		ArticleInfo object = new ArticleInfo();
		for (String field : schemaFields) {
			Object value = key.datum().get(field);
			if (null != value) {
				setValue(object, field, value);
			}
		}
		return object;
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
