package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AVROToParquetEngine extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	public static void main(String[] args) throws Exception {
		if (2 != args.length) {
			System.exit(0);
		}
		int res = ToolRunner.run(new AVROToParquetEngine(), args);
		System.exit(res);
	}
}
