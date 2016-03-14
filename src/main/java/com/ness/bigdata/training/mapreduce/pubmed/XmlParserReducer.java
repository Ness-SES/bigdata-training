package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class XmlParserReducer extends Reducer<IntWritable, ArticleInfo, AvroKey<Integer>, AvroValue<GenericRecord>> {
    @Override
    protected void reduce(IntWritable key, Iterable<ArticleInfo> values,
            Reducer<IntWritable, ArticleInfo, AvroKey<Integer>, AvroValue<GenericRecord>>.Context context)
            throws IOException, InterruptedException {
        for (ArticleInfo val : values) {
            context.write(new AvroKey<Integer>(key.get()), new AvroValue<GenericRecord>(val.toAvroGenericRecord()));
        }
    }
}
