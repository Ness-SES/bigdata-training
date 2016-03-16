package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;

public class AVROToParquetEngine extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration config = new Configuration();

		FileSystem fs = FileSystem.get(config);
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		Job job = Job.getInstance(config, AVROToParquetEngine.class.getSimpleName());

		job.setJarByClass(AVROToParquetEngine.class);
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setOutputFormatClass(ParquetOutputFormat.class);

		job.setMapperClass(AVROInputMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(AVROToParquetArrayWritable.class);

		job.setReducerClass(ParquetOutputReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(AVROToParquetArrayWritable.class);

		if (3 == args.length) {
			Integer reducers = Integer.valueOf(args[2]);
			job.setNumReduceTasks(reducers);
		}

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));
		ParquetOutputFormat.setOutputPath(job, new Path(args[1]));

		ParquetOutputFormat.setWriteSupportClass(job, DataWritableWriteSupport.class);
		ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
		ParquetOutputFormat.setBlockSize(job, 128 * 1024 * 1024);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		if (2 > args.length) {
			System.exit(0);
		}
		int res = ToolRunner.run(new AVROToParquetEngine(), args);
		System.exit(res);
	}
}
