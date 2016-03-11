package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class XmlParserPlainReducer extends Reducer<IntWritable, ArticleInfo, IntWritable, ArticleInfo> {
	@Override
	protected void reduce(IntWritable key, Iterable<ArticleInfo> values,
			Reducer<IntWritable, ArticleInfo, IntWritable, ArticleInfo>.Context context)
			throws IOException, InterruptedException {
		for (ArticleInfo val : values) {
			context.write(key, val);
		}
	}
}
