package com.ness.bigdata.training.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.ness.bigdata.training.mapreduce.wordcount.WordCountMapper;
import com.ness.bigdata.training.mapreduce.wordcount.WordCountReducer;

public class WordCountJobTest {
	private MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setup() {
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(new WordCountMapper(), new WordCountReducer());
	}

	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.withInput(new IntWritable(), new Text("test 123 inca un test"))
				.withOutput(new Text("123"), new IntWritable(1)).withOutput(new Text("inca"), new IntWritable(1))
				.withOutput(new Text("test"), new IntWritable(2)).withOutput(new Text("un"), new IntWritable(1))
				.runTest();
	}
}
