package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.ness.bigdata.training.mapreduce.pubmed.ArticleInfo;

public class ParquetOutputReducer extends Reducer<ArticleInfo, IntWritable, Text, NullWritable> {

	@Override
	protected void reduce(ArticleInfo key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(key.toString()), NullWritable.get());
	}
}
