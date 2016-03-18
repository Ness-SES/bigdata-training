package com.ness.bigdata.training.mapreduce.linecounter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LineCounterMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable ONE = new IntWritable(1);

	enum CounterNames {
		LINES_LONGER_THAN_10_CHARS, LINES_SHORTER_THAN_10_CHARS
	}

	@Override
	public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		boolean isLineLongerThan10Chars = (value.getLength() > 10);
		context.getCounter(isLineLongerThan10Chars ? CounterNames.LINES_LONGER_THAN_10_CHARS
				: CounterNames.LINES_SHORTER_THAN_10_CHARS).increment(1);
		if (isLineLongerThan10Chars) {
			context.write(value, ONE);
		}
	}
}
