package com.ness.bigdata.training.mapreduce.pubmed;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class XmlParserJob {
	static final Schema SCHEMA = new Schema.Parser().parse("{"+
"\"type\": \"record\","+
"\"name\": \"ArticleInfo\","+
"\"fields\": ["+
"	{\"name\": \"filePath\", \"type\": \"string\"},"+
"	{\"name\": \"articleTitle\", \"type\": \"string\"},"+
"	{\"name\": \"articlePublisherId\", \"type\": \"long\"},"+
"	{\"name\": \"articleIssnPPub\", \"type\": \"string\"},"+
"	{\"name\": \"articleDateAccepted\", \"type\": \"long\"}"+
"]"+
"}");
	
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, XmlParserJob.class.getSimpleName());
        job.setJarByClass(XmlParserJob.class);
        job.setMapperClass(XmlParserMapper.class);
        job.setReducerClass(XmlParserReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(ArticleInfo.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setOutputValueSchema(job, SCHEMA);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
