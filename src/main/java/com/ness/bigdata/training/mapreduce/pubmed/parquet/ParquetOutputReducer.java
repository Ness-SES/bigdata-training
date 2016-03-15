package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParquetOutputReducer extends Reducer<AVROToParquetArrayWritable, NullWritable, Text, NullWritable> {

	@Override
	protected void reduce(AVROToParquetArrayWritable key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(key.toString()), NullWritable.get());
	}
}
