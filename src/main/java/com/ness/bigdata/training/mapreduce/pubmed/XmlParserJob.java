package com.ness.bigdata.training.mapreduce.pubmed;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class XmlParserJob extends Configured {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String extraArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

        Job job = Job.getInstance(conf, XmlParserJob.class.getSimpleName());
        job.setJarByClass(XmlParserJob.class);

        FileInputFormat.addInputPath(job, new Path(extraArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(extraArgs[1]));
        
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(XmlParserMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setReducerClass(XmlParserReducer.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
        Schema schema = AvroSchemaLoader.getInstance(conf, Constants.CONFIG_KEY_AVRO_SCHEMA_FILE_PATH).getSchema();
        AvroJob.setOutputValueSchema(job, schema);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
