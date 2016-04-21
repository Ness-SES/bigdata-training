package com.ness.bigdata.training.mapreduce.linecounter;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.ness.bigdata.training.mapreduce.linecounter.LineCounterReducer;

public class LineCounterReducerTest {
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

	@Before
	public void setup() {
		reduceDriver = ReduceDriver.newReduceDriver(new LineCounterReducer());
	}

	@Test
	public void testReducer() throws IOException {
		reduceDriver
				.withInput(new Text("linie mai lunga de 10 caractere"),
						Arrays.asList(new IntWritable(1), new IntWritable(1)))
				.withInput(new Text("linie foarte foarte lunga"), Arrays.asList(new IntWritable(1)))
				.withOutput(new Text("linie mai lunga de 10 caractere"), new IntWritable(2))
				.withOutput(new Text("linie foarte foarte lunga"), new IntWritable(1))
				.withCounter(LineCounterReducer.CounterNames.LINES_CONTAINING_DIGITS, 2).runTest();
	}
}
