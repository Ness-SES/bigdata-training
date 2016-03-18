package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

public class FieldMetaLoader {
    private static final FieldMeta FIELD_META_FILE_PATH = new FieldMeta(Constants.FIELD_NAME_FILE_PATH, null,
            FieldMeta.Type.STRING);

    private static FieldMetaLoader instance;

    private final List<FieldMeta> fieldMeta;
    private final Schema schema;

    private FieldMetaLoader(Configuration configuration, String configurationSettingName) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path filePath = new Path(configuration.get(configurationSettingName));
        fieldMeta = loadArticleInfo(fileSystem, filePath);
        schema = generateAvroSchema();
    }

    private List<FieldMeta> loadArticleInfo(FileSystem fileSystem, Path filePath) throws IOException {
        try (InputStream stream = fileSystem.open(filePath).getWrappedStream()) {
            Gson gson = new Gson();
            JsonReader reader = new JsonReader(new InputStreamReader(stream));
            return gson.fromJson(reader, new TypeToken<List<FieldMeta>>() {
            }.getType());
        }
    }

    private Schema generateAvroSchema() {
        FieldAssembler<Schema> fields = SchemaBuilder.record("ArticleInfo").fields();
        fields = fields.name(FIELD_META_FILE_PATH.getName()).type(FIELD_META_FILE_PATH.getAvroType()).noDefault();
        for (FieldMeta oneFieldMeta : fieldMeta) {
            fields = fields.name(oneFieldMeta.getName()).type(oneFieldMeta.getAvroType()).noDefault();
        }
        return fields.endRecord();
    }

    public List<FieldMeta> getFieldMeta() {
        return fieldMeta;
    }

    public Schema getAvroSchema() {
        return schema;
    }

    public FieldMeta getFieldMeta(String fieldName) {
        if (Constants.FIELD_NAME_FILE_PATH.equals(fieldName)) {
            return FIELD_META_FILE_PATH;
        } else {
            for (FieldMeta oneFieldMeta : fieldMeta) {
                if (fieldName.equals(oneFieldMeta.getName())) {
                    return oneFieldMeta;
                }
            }
            throw new NoSuchElementException(fieldName);
        }
    }

    public static FieldMetaLoader getInstance(Configuration configuration, String configurationSettingName)
            throws IOException {
        if (null == instance) {
            instance = new FieldMetaLoader(configuration, configurationSettingName);
        }
        return instance;
    }
}
