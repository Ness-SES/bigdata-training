package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ParquetOutputReducer
		extends Reducer<NullWritable, AVROToParquetArrayWritable, NullWritable, AVROToParquetArrayWritable> {

	@Override
	protected void reduce(NullWritable key, Iterable<AVROToParquetArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		for (AVROToParquetArrayWritable resultedData : values) {
			context.write(NullWritable.get(), resultedData);
		}
	}
}
