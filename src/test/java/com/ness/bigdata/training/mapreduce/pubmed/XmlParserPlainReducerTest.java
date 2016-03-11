package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class XmlParserPlainReducerTest {
	private ReduceDriver<IntWritable, ArticleInfo, IntWritable, ArticleInfo> reduceDriver;

	@Before
	public void setup() {
		reduceDriver = ReduceDriver.newReduceDriver(new XmlParserPlainReducer());
	}

	@Test
	public void testReducer() throws IOException {
		Calendar calendar = Calendar.getInstance();
		calendar.set(2011, 9 - 1, 28, 0, 0, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		ArticleInfo articleInfo = new ArticleInfo(
				"/user/ubuntu/datasets/pubmed/unzipped/unzipped.A-B/3_Biotech/3_Biotech_2011_Dec_13_1(4)_217-225.nxml",
				"Evaluation of indigenous Trichoderma isolates from Manipur as biocontrol agent against Pythium aphanidermatum on common beans",
				27L, "2190-572X", calendar.getTimeInMillis());
		reduceDriver.withInput(new IntWritable(1), Arrays.asList(articleInfo))
				.withOutput(new IntWritable(1), articleInfo).runTest();
	}
}
