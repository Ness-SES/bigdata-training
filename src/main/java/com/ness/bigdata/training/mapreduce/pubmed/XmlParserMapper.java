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

public class XmlParserMapper extends Mapper<Object, Text, IntWritable, MapWritable> {
    private final static IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, MapWritable>.Context context)
            throws IOException, InterruptedException {
        String xmlFilePath = value.toString().split("\t")[1];
        MapWritable output = extractArticleInfo(xmlFilePath, context.getConfiguration());
        output.put(new Text(Constants.AVRO_FIELD_FILE_PATH), new Text(xmlFilePath));
        context.write(ONE, output);
    }

    private MapWritable extractArticleInfo(String xmlFilePath, Configuration configuration) throws IOException {
        PropertiesLoader propertiesLoader = PropertiesLoader.getInstance(configuration,
                Constants.CONFIG_KEY_AVRO_2_XPATH_MAPPING_FILE_PATH);
        XmlParser xmlParser = parseXml(FileSystem.get(configuration), new Path(xmlFilePath));
        return extractArticleInfo(propertiesLoader, xmlParser);
    }

    private MapWritable extractArticleInfo(PropertiesLoader propertiesLoader, XmlParser xmlParser) {
        MapWritable output = new MapWritable();
        for (String fieldName : propertiesLoader.getPropertyNames()) {
            String xPath = propertiesLoader.getProperty(fieldName);
            String fieldValue = xmlParser.evaluate(xPath);
            output.put(new Text(fieldName), new Text(fieldValue));
        }
        return output;
    }

    private XmlParser parseXml(FileSystem fileSystem, Path filePath) throws IOException {
        try (InputStream stream = fileSystem.open(filePath).getWrappedStream()) {
            return new XmlParser(stream);
        }
    }
}
