package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PrepareForTest(FileSystem.class)
@RunWith(PowerMockRunner.class)
public class XmlParserMapperTest {
	private MapDriver<Object, Text, IntWritable, ArticleInfo> mapDriver;

	@Mock
	private FileSystem fileSystemMock;

	@Mock
	private FSDataInputStream fsDataInputStreamMock;

	private InputStream stream;

	@Before
	public void setUp() throws IOException {
		PowerMockito.mockStatic(FileSystem.class);
		PowerMockito.when(FileSystem.get(Mockito.any(Configuration.class))).thenReturn(fileSystemMock);

		Mockito.when(fileSystemMock.open(Mockito.any(Path.class))).thenReturn(fsDataInputStreamMock);

		stream = new FileInputStream(new File("src/test/resources/3_Biotech_2011_Dec_13_1(4)_217-225.xml"));
		Mockito.when(fsDataInputStreamMock.getWrappedStream()).thenReturn(stream);

		mapDriver = MapDriver.newMapDriver(new XmlParserMapper());
	}

	@After
	public void tearDown() throws IOException {
		stream.close();
	}

	@Test
	public void test() throws IOException {
        String filePath = "/user/ubuntu/datasets/pubmed/unzipped/unzipped.A-B/3_Biotech/3_Biotech_2011_Dec_13_1(4)_217-225.nxml";
        String line = "1\t" + filePath;
		Calendar calendar = Calendar.getInstance();
		calendar.set(2011, 9 - 1, 28, 0, 0, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		ArticleInfo expectedArticleInfo = new ArticleInfo(filePath,
				"Evaluation of indigenous Trichoderma isolates from Manipur as biocontrol agent against Pythium aphanidermatum on common beans",
				27L, "2190-572X", calendar.getTimeInMillis());
		mapDriver.withInput(NullWritable.get(), new Text(line)).withOutput(new IntWritable(1), expectedArticleInfo)
				.runTest();
	}
}
