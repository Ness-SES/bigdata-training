package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PrepareForTest(FileSystem.class)
@RunWith(PowerMockRunner.class)
public class XmlParserFileContentsMapperTest {
	private MapDriver<Object, Text, IntWritable, ArticleInfo> mapDriver;

	@Before
	public void setUp() throws IOException {
		mapDriver = MapDriver.newMapDriver(new XmlParserFileContentsMapper());
	}

	@Test
	public void test() throws IOException {
		String fileContents = FileUtils.readFileToString(new File("src/test/resources/3_Biotech_2011_Dec_13_1(4)_217-225.xml"));
		Calendar calendar = Calendar.getInstance();
		calendar.set(2011, 9 - 1, 28, 0, 0, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		ArticleInfo expectedArticleInfo = new ArticleInfo("",
				"Evaluation of indigenous Trichoderma isolates from Manipur as biocontrol agent against Pythium aphanidermatum on common beans",
				27L, "2190-572X", calendar.getTimeInMillis());
		mapDriver.withInput(NullWritable.get(), new Text(fileContents)).withOutput(new IntWritable(1), expectedArticleInfo)
				.runTest();
	}
}
