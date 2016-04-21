package com.ness.bigdata.training.mapreduce.linecounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LineCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private final IntWritable result = new IntWritable();

	enum CounterNames {
		LINES_CONTAINING_DIGITS
	}

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
		if (containsDigit(key.toString())) {
			context.getCounter(CounterNames.LINES_CONTAINING_DIGITS).increment(sum);
		}
	}

	private boolean containsDigit(String line) {
		return line.matches(".*\\d+.*");
	}
}
