package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class XmlParserMapper extends Mapper<Object, Text, IntWritable, ArticleInfo> {
	private final static IntWritable ONE = new IntWritable(1);

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, ArticleInfo>.Context context)
			throws IOException, InterruptedException {
		String filePath = value.toString();
		XmlParser fileParser = parseFile(new Path(filePath), context.getConfiguration());
		ArticleInfo articleInfo = new ArticleInfo(filePath, fileParser.getTitle(), fileParser.getPublisherId(),
				fileParser.getIssnPPub(), fileParser.getAcceptedDate());
		context.write(ONE, articleInfo);
	}

	private XmlParser parseFile(Path filePath, Configuration configuration) throws IOException {
		FileSystem fileSystem = FileSystem.get(configuration);
		try (InputStream stream = fileSystem.open(filePath).getWrappedStream()) {
			return new XmlParser(stream);
		}
	}
}
