package com.ness.bigdata.training.mapreduce.wordcount;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.ness.bigdata.training.mapreduce.wordcount.WordCountReducer;

public class WordCountReducerTest {
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

	@Before
	public void setup() {
		reduceDriver = ReduceDriver.newReduceDriver(new WordCountReducer());
	}

	@Test
	public void testReducer() throws IOException {
		reduceDriver.withInput(new Text("test"), Arrays.asList(new IntWritable(1), new IntWritable(1)))
				.withOutput(new Text("test"), new IntWritable(2)).runTest();
	}
}
