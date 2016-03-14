package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ness.bigdata.training.mapreduce.pubmed.ArticleInfo;

public class AVROToParquetEngine extends Configured implements Tool {

	static final Schema SCHEMA;

	static {
		try {
			InputStream resourceAsStream = Thread.currentThread().getContextClassLoader()
					.getResourceAsStream("ArticleInfo.avsc");
			SCHEMA = new Schema.Parser().parse(resourceAsStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
        		Configuration config = new Configuration();
		config.set(Constants.SCHEMA_KEY, SCHEMA.toString());

		FileSystem fs = FileSystem.get(config);
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		Job job = Job.getInstance(config, AVROToParquetEngine.class.getSimpleName());
		job.setJarByClass(AVROToParquetEngine.class);
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(AVROInputMapper.class);
		AvroJob.setInputKeySchema(job, SCHEMA);
		job.setMapOutputKeyClass(ArticleInfo.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(ParquetOutputReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		Integer reducers = Integer.valueOf(args[2]);
		job.setNumReduceTasks(reducers);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		if (3 != args.length || null == SCHEMA) {
			System.exit(0);
		}
		int res = ToolRunner.run(new AVROToParquetEngine(), args);
		System.exit(res);
	}
}
