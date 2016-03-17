package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

public class ParquetOutputReducer
		extends Reducer<NullWritable, AVROToParquetArrayWritable, Void, ArrayWritable> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		DataWritableWriteSupport.getSchema(context.getConfiguration());
	}

	@Override
	protected void reduce(NullWritable key, Iterable<AVROToParquetArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		for (AVROToParquetArrayWritable resultedData : values) {
			if (null != resultedData && null != resultedData.get() && 0 < resultedData.get().length) {
				MessageType parquetSchema = initializeSchema(resultedData);
				if (null != parquetSchema) {
					DataWritableWriteSupport.setSchema(parquetSchema, context.getConfiguration());
					context.write(null, resultedData.getArrayWritable());
				}
			}
		}
	}

	private MessageType initializeSchema(AVROToParquetArrayWritable resultedData) {
		return MessageTypeParser.parseMessageType(resultedData.getSchema());
	}
}
