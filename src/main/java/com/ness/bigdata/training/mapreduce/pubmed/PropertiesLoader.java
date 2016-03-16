package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PropertiesLoader {
    private final Properties properties;
    private static PropertiesLoader instance;

    private PropertiesLoader(Configuration configuration, String configurationSettingName) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path filePath = new Path(configuration.get(configurationSettingName));
        properties = loadProperties(fileSystem, filePath);
    }

    private Properties loadProperties(FileSystem fileSystem, Path filePath) throws IOException {
        try (InputStream stream = fileSystem.open(filePath).getWrappedStream()) {
            Properties properties = new Properties();
            properties.load(stream);
            return properties;
        }
    }

    public Set<String> getPropertyNames() {
        return properties.stringPropertyNames();
    }
    
    public String getProperty(String name) {
        return properties.getProperty(name);
    }

    public static PropertiesLoader getInstance(Configuration configuration, String configurationSettingName)
            throws IOException {
        if (null == instance) {
            instance = new PropertiesLoader(configuration, configurationSettingName);
        }
        return instance;
    }
}
