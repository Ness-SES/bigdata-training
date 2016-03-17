package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import org.apache.avro.Schema;
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

import parquet.avro.AvroSchemaConverter;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

public class AvroToParquetEngine extends Configured implements Tool {

	private static final Schema SCHEMA = new Schema.Parser().parse("{" + "\"type\": \"record\","
			+ "\"name\": \"ArticleInfo\"," + "\"fields\": [" + "	{\"name\": \"filePath\", \"type\": \"string\"},"
			+ "	{\"name\": \"articleTitle\", \"type\": \"string\"},"
			+ "	{\"name\": \"articlePublisherId\", \"type\": \"long\"},"
			+ "	{\"name\": \"articleIssnPPub\", \"type\": \"string\"},"
			+ "	{\"name\": \"articleDateAccepted\", \"type\": \"long\"}" + "]" + "}");

	@Override
	public int run(String[] args) throws Exception {
		Configuration config = new Configuration();

		FileSystem fs = FileSystem.get(config);
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		config.set(ParquetOutputFormat.COMPRESSION, "SNAPPY");
		config.set(ParquetOutputFormat.BLOCK_SIZE, Integer.toString(128 * 1024 * 1024));

		/*
		 * instantiate dummy schema to avoid NullPointerException at reducer
		 * initialization
		 */
		MessageType mt = new AvroSchemaConverter().convert(SCHEMA);

		DataWritableWriteSupport.setSchema(mt, config);

		Job job = Job.getInstance(config, AvroToParquetEngine.class.getSimpleName());

		job.setJarByClass(AvroToParquetEngine.class);
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setOutputFormatClass(ParquetOutputFormat.class);

		job.setMapperClass(AvroInputMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(AvroToParquetArrayWritable.class);

		job.setReducerClass(ParquetOutputReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(AvroToParquetArrayWritable.class);

		if (3 == args.length)

		{
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
		int res = ToolRunner.run(new AvroToParquetEngine(), args);
		System.exit(res);
	}
}
