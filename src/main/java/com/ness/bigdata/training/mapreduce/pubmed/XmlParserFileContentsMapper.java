package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class XmlParserFileContentsMapper extends Mapper<Object, Text, IntWritable, ArticleInfo> {
	private final static IntWritable ONE = new IntWritable(1);

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, ArticleInfo>.Context context)
			throws IOException, InterruptedException {
		InputStream fileContents = IOUtils.toInputStream(value.toString());
		XmlParser fileParser = new XmlParser(fileContents);
		ArticleInfo articleInfo = new ArticleInfo("", fileParser.getTitle(), fileParser.getPublisherId(),
				fileParser.getIssnPPub(), fileParser.getAcceptedDate());
		context.write(ONE, articleInfo);
	}
}
