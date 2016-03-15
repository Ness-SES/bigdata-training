package com.ness.bigdata.training.mapreduce.pubmed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.InputStream;

public class XmlParserMapper extends Mapper<Text, Text, IntWritable, ArticleInfo> {
	private final static IntWritable ONE = new IntWritable(1);

    	@Override
	protected void map(Text fileSize, Text filePath, Context context) throws IOException, InterruptedException {
		XmlParser fileParser = parseFile(new Path(filePath.toString()), context.getConfiguration());
		ArticleInfo articleInfo = new ArticleInfo(filePath.toString(), fileParser.getTitle(), fileParser.getPublisherId(),
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
