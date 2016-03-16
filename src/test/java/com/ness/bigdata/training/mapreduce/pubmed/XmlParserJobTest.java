package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PrepareForTest(FileSystem.class)
@RunWith(PowerMockRunner.class)
public class XmlParserJobTest {
    private MapReduceDriver<Text, Text, IntWritable, MapWritable, AvroKey<Integer>, AvroValue<GenericRecord>> mapReduceDriver;

    @Mock
    private FileSystem fileSystemMock;

    @Mock
    private FSDataInputStream xmlFSDataInputStreamMock;

    @Mock
    private FSDataInputStream avsc2xmlPropertiesFSDataInputStreamMock;

    @Mock
    private FSDataInputStream avscFSDataInputStreamMock;

    private InputStream xmlStream;
    private InputStream avsc2xmlPropertiesStream;
    private InputStream avscStream;

    @Before
    public void setUp() throws IOException {
        xmlStream = new FileInputStream(new File(TestData.XML));
        avsc2xmlPropertiesStream = new FileInputStream(new File(TestData.AVSC2XPATH_PROPERTIES));
        avscStream = new FileInputStream(new File(TestData.AVSC));

        PowerMockito.mockStatic(FileSystem.class);
        PowerMockito.when(FileSystem.get(Mockito.any(Configuration.class))).thenReturn(fileSystemMock);

        Mockito.when(fileSystemMock.open(new Path(TestData.DUMMY_HDFS_PATH_XML))).thenReturn(xmlFSDataInputStreamMock);
        Mockito.when(xmlFSDataInputStreamMock.getWrappedStream()).thenReturn(xmlStream);

        Mockito.when(fileSystemMock.open(new Path(TestData.DUMMY_HDFS_PATH_AVSC2XPATH_PROPERTIES)))
                .thenReturn(avsc2xmlPropertiesFSDataInputStreamMock);
        Mockito.when(avsc2xmlPropertiesFSDataInputStreamMock.getWrappedStream()).thenReturn(avsc2xmlPropertiesStream);

        Mockito.when(fileSystemMock.open(new Path(TestData.DUMMY_HDFS_PATH_AVSC)))
                .thenReturn(avscFSDataInputStreamMock);
        Mockito.when(avscFSDataInputStreamMock.getWrappedStream()).thenReturn(avscStream);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(new XmlParserMapper(), new XmlParserReducer());
        Configuration driverConfiguration = mapReduceDriver.getConfiguration();

        String[] ioSerializations = driverConfiguration.getStrings("io.serializations");
        String[] newIOSerializations = new String[ioSerializations.length + 1];
        System.arraycopy(ioSerializations, 0, newIOSerializations, 0, ioSerializations.length);
        newIOSerializations[newIOSerializations.length - 1] = AvroSerialization.class.getName();

        driverConfiguration.setStrings("io.serializations", newIOSerializations);
        driverConfiguration.set("avro.serialization.value.writer.schema", TestData.AVRO_SCHEMA.toString());
        driverConfiguration.set("avro.serialization.key.writer.schema", Schema.create(Schema.Type.INT).toString(true));
        driverConfiguration.set(Constants.CONFIG_KEY_AVRO_2_XPATH_MAPPING_FILE_PATH,
                TestData.DUMMY_HDFS_PATH_AVSC2XPATH_PROPERTIES);
        driverConfiguration.set(Constants.CONFIG_KEY_AVRO_SCHEMA_FILE_PATH, TestData.DUMMY_HDFS_PATH_AVSC);
    }

    @After
    public void tearDown() throws IOException {
        xmlStream.close();
        avsc2xmlPropertiesStream.close();
        avscStream.close();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new Text("1"), new Text(TestData.DUMMY_HDFS_PATH_XML))
                .withOutput(new AvroKey<Integer>(1), new AvroValue<GenericRecord>(TestData.ARTICLE_INFO_GENERIC_RECORD))
                .runTest();
    }
}
