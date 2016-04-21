package com.ness.bigdata.training.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.ness.bigdata.training.mapreduce.wordcount.WordCountMapper;

public class WordCountMapperTest {
	private MapDriver<Object, Text, Text, IntWritable> mapDriver;

	@Before
	public void setup() {
		mapDriver = MapDriver.newMapDriver(new WordCountMapper());
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new IntWritable(), new Text("test 123 inca un test"))
				.withOutput(new Text("test"), new IntWritable(1)).withOutput(new Text("123"), new IntWritable(1))
				.withOutput(new Text("inca"), new IntWritable(1)).withOutput(new Text("un"), new IntWritable(1))
				.withOutput(new Text("test"), new IntWritable(1)).runTest();
	}
}
