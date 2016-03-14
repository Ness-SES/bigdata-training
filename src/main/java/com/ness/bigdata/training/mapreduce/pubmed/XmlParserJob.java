package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class XmlParserJob {
    static final Schema SCHEMA; 
    
    static {
        try {
            InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("ArticleInfo.avsc");
            SCHEMA = new Schema.Parser().parse(resourceAsStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
	
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String extraArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());
        
        Job job = Job.getInstance(conf, XmlParserJob.class.getSimpleName());
        job.setJarByClass(XmlParserJob.class);
        
        FileInputFormat.addInputPath(job, new Path(extraArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(extraArgs[1]));
        
        job.setMapperClass(XmlParserMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ArticleInfo.class);
        
        job.setReducerClass(XmlParserReducer.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setOutputValueSchema(job, SCHEMA);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
