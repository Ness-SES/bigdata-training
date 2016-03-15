package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
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
public class XmlParserMapperTest {

    private MapDriver<Object, Text, IntWritable, MapWritable> mapDriver;

    @Mock
    private FileSystem fileSystemMock;

    @Mock
    private FSDataInputStream xmlFSDataInputStreamMock;

    @Mock
    private FSDataInputStream avsc2xmlPropertiesFSDataInputStreamMock;

    private InputStream xmlStream;
    private InputStream avsc2xmlPropertiesStream;

    @Before
    public void setUp() throws IOException {
        xmlStream = new FileInputStream(new File(TestData.XML));
        avsc2xmlPropertiesStream = new FileInputStream(new File(TestData.AVSC2XPATH_PROPERTIES));

        PowerMockito.mockStatic(FileSystem.class);
        PowerMockito.when(FileSystem.get(Mockito.any(Configuration.class))).thenReturn(fileSystemMock);

        Mockito.when(fileSystemMock.open(new Path(TestData.DUMMY_HDFS_PATH_XML))).thenReturn(xmlFSDataInputStreamMock);
        Mockito.when(xmlFSDataInputStreamMock.getWrappedStream()).thenReturn(xmlStream);

        Mockito.when(fileSystemMock.open(new Path(TestData.DUMMY_HDFS_PATH_AVSC2XPATH_PROPERTIES)))
                .thenReturn(avsc2xmlPropertiesFSDataInputStreamMock);
        Mockito.when(avsc2xmlPropertiesFSDataInputStreamMock.getWrappedStream()).thenReturn(avsc2xmlPropertiesStream);

        mapDriver = MapDriver.newMapDriver(new XmlParserMapper());
        mapDriver.getConfiguration().set(Constants.CONFIG_KEY_AVRO_2_XPATH_MAPPING_FILE_PATH,
                TestData.DUMMY_HDFS_PATH_AVSC2XPATH_PROPERTIES);
    }

    @After
    public void tearDown() throws IOException {
        xmlStream.close();
        avsc2xmlPropertiesStream.close();
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(NullWritable.get(), new Text("1\t" + TestData.DUMMY_HDFS_PATH_XML))
                .withOutput(new IntWritable(1), TestData.ARTICLE_INFO_MAP).runTest();
    }
}
