package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class XmlParserReducer extends Reducer<IntWritable, MapWritable, AvroKey<Integer>, AvroValue<GenericRecord>> {
    @Override
    protected void reduce(IntWritable key, Iterable<MapWritable> values,
            Reducer<IntWritable, MapWritable, AvroKey<Integer>, AvroValue<GenericRecord>>.Context context)
            throws IOException, InterruptedException {
        for (MapWritable value : values) {
            GenericRecord genericRecord = toGenericRecord(value, context.getConfiguration());
            context.write(new AvroKey<Integer>(key.get()), new AvroValue<GenericRecord>(genericRecord));
        }
    }

    private GenericRecord toGenericRecord(MapWritable articleInfo, Configuration conf) throws IOException {
        Schema schema = AvroSchemaLoader.getInstance(conf, Constants.CONFIG_KEY_AVRO_SCHEMA_FILE_PATH).getSchema();
        GenericRecord genericRecord = new GenericData.Record(schema);
        for (Entry<Writable, Writable> entry : articleInfo.entrySet()) {
            genericRecord.put(entry.getKey().toString(), entry.getValue());
        }
        return genericRecord;
    }
}
