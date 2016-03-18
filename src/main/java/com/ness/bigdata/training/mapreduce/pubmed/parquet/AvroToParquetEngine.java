package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ness.bigdata.training.mapreduce.pubmed.Constants;

import parquet.avro.AvroSchemaConverter;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

public class AvroToParquetEngine extends Configured implements Tool {

	private static final Schema SCHEMA = SchemaBuilder.record("ArticleInfo").fields()
			.name(Constants.FIELD_NAME_FILE_PATH).type().stringType().noDefault().name("articleTitle").type()
			.stringType().noDefault().name("articlePublisherId").type().intType().noDefault().name("articleIssnPPub")
			.type().stringType().noDefault().name("articleDateAccepted").type().longType().noDefault().endRecord();

	@Override
	public int run(String[] args) throws Exception {
		Configuration config = new Configuration();

		FileSystem fs = FileSystem.get(config);
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		config.set(ParquetOutputFormat.COMPRESSION, "SNAPPY");
		config.set(ParquetOutputFormat.BLOCK_SIZE, Integer.toString(128 * 1024 * 1024));

		MessageType mt = new AvroSchemaConverter().convert(SCHEMA);
		DataWritableWriteSupport.setSchema(mt, config);

		config.set("parquet.from.avro.schema", SCHEMA.toString());

		Job job = Job.getInstance(config, AvroToParquetEngine.class.getSimpleName());

		job.setJarByClass(AvroToParquetEngine.class);
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setOutputFormatClass(ParquetOutputFormat.class);

		job.setMapperClass(AvroInputMapper.class);
		job.setMapOutputKeyClass(Void.class);
		job.setMapOutputValueClass(ArrayWritable.class);

		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
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
