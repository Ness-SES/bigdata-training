package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvroInputMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, NullWritable, ArrayWritable> {

    private static Schema schema;
    private static Writable[] resultedDataArray;

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        initializeSchemas(context.getConfiguration());
        if (instantiateData(key.datum(), context.getConfiguration())) {
            context.write(null, new ArrayWritable(Writable.class, resultedDataArray));
        }
    }

    private void initializeSchemas(Configuration configuration) {
        if (null == schema) {
            String strSchema = configuration.get("parquet.from.avro.schema");
            schema = new Schema.Parser().parse(strSchema);
            createParquetSchema(configuration);
        }
    }

    private boolean instantiateData(GenericRecord data, Configuration configuration) {
        if (null == data || null == data.getSchema() || null == data.getSchema().getFields()
                || data.getSchema().getFields().isEmpty() || null == configuration) {
            return false;
        }
        boolean dataInstantiated = false;
        for (Field field : schema.getFields()) {
            switch (field.schema().getType()) {
                case STRING:
                    String strValue = (String) data.get(field.name());
                    if (null != strValue) {
                        resultedDataArray[field.pos()] = new BytesWritable(data.get(field.name()).toString().getBytes());
                        // resultedDataArray[field.pos()] = new BinaryWritable(Binary.fromString((String) data.get(field.name())));
                    }
                    break;
                case LONG:
                    Long longValue = (Long) data.get(field.name());
                    if (null != longValue) {
                        resultedDataArray[field.pos()] = new LongWritable((Long) data.get(field.name()));
                    }
                    break;
                case INT:
                    Integer intValue = (Integer) data.get(field.name());
                    if (null != intValue) {
                        resultedDataArray[field.pos()] = new IntWritable((Integer) data.get(field.name()));
                    }
                    break;
                default:
                    break;
            }

            dataInstantiated = true;
        }
        return dataInstantiated;
    }

    private void createParquetSchema(Configuration config) {
        if (null == schema || null == schema.getFields() || schema.getFields().isEmpty()) {
            return;
        }
        resultedDataArray = new Writable[schema.getFields().size()];
    }
}
