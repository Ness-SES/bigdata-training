package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AvroSchemaLoader {
    private final Schema schema;
    private static AvroSchemaLoader instance;

    private AvroSchemaLoader(Configuration configuration, String configurationSettingName) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path filePath = new Path(configuration.get(configurationSettingName));
        schema = loadSchema(fileSystem, filePath);
    }

    private Schema loadSchema(FileSystem fileSystem, Path filePath) throws IOException {
        try (InputStream stream = fileSystem.open(filePath).getWrappedStream()) {
            return new Schema.Parser().parse(stream);
        }
    }
    
    public Schema getSchema() {
        return schema;
    }

    public static AvroSchemaLoader getInstance(Configuration configuration, String configurationSettingName)
            throws IOException {
        if (null == instance) {
            instance = new AvroSchemaLoader(configuration, configurationSettingName);
        }
        return instance;
    }
}
