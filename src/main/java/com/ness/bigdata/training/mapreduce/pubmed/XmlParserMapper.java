package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class XmlParserMapper extends Mapper<Text, Text, IntWritable, MapWritable> {
    private final static IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, IntWritable, MapWritable>.Context context)
            throws IOException, InterruptedException {
        String xmlFilePath = value.toString();
        MapWritable output = extractArticleInfo(xmlFilePath, context.getConfiguration());
        output.put(new Text(Constants.FIELD_NAME_FILE_PATH), new Text(xmlFilePath));
        context.write(ONE, output);
    }

    private MapWritable extractArticleInfo(String xmlFilePath, Configuration configuration) throws IOException {
        FieldMetaLoader fieldMetaLoader = FieldMetaLoader.getInstance(configuration, Constants.CONFIG_KEY_FIELD_META_FILE_PATH);
        XmlParser xmlParser = parseXml(FileSystem.get(configuration), new Path(xmlFilePath));
        return extractArticleInfo(fieldMetaLoader, xmlParser);
    }

    private MapWritable extractArticleInfo(FieldMetaLoader fieldMetaLoader, XmlParser xmlParser) {
        MapWritable output = new MapWritable();
        for (FieldMeta oneFieldMeta : fieldMetaLoader.getFieldMeta()) {
            output.put(new Text(oneFieldMeta.getName()), xmlParser.evaluate(oneFieldMeta));
        }
        return output;
    }

    private XmlParser parseXml(FileSystem fileSystem, Path filePath) throws IOException {
        try (InputStream stream = fileSystem.open(filePath).getWrappedStream()) {
            return new XmlParser(stream);
        }
    }
}
