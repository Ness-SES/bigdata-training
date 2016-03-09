package com.ness.bigdata.training.mapreduce.linecounter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.ness.bigdata.training.mapreduce.linecounter.LineCounterMapper;

public class LineCounterMapperTest {
	private MapDriver<Object, Text, Text, IntWritable> mapDriver;

	@Before
	public void setup() {
		mapDriver = MapDriver.newMapDriver(new LineCounterMapper());
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new IntWritable(), new Text("linie mai lunga de 10 caractere"))
				.withInput(new IntWritable(), new Text("linie si mai lunga de 10 caractere"))
				.withInput(new IntWritable(), new Text("linie"))
				.withOutput(new Text("linie mai lunga de 10 caractere"), new IntWritable(1))
				.withOutput(new Text("linie si mai lunga de 10 caractere"), new IntWritable(1))
				.withCounter(LineCounterMapper.CounterNames.LINES_LONGER_THAN_10_CHARS, 2)
				.withCounter(LineCounterMapper.CounterNames.LINES_SHORTER_THAN_10_CHARS, 1).runTest();
	}

}
