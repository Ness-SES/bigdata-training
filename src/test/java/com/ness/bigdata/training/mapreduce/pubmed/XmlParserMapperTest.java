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
    private MapDriver<Text, Text, IntWritable, MapWritable> mapDriver;

    @Mock
    private FileSystem fileSystemMock;

    @Mock
    private FSDataInputStream xmlFSDataInputStreamMock;

    @Mock
    private FSDataInputStream fieldMetaFSDataInputStreamMock;

    private InputStream xmlStream;
    private InputStream fieldMetaStream;

    @Before
    public void setUp() throws IOException {
        xmlStream = new FileInputStream(new File(TestData.XML));
        fieldMetaStream = new FileInputStream(new File(TestData.FIELD_META));

        PowerMockito.mockStatic(FileSystem.class);
        PowerMockito.when(FileSystem.get(Mockito.any(Configuration.class))).thenReturn(fileSystemMock);

        Mockito.when(fileSystemMock.open(new Path(TestData.DUMMY_HDFS_PATH_XML))).thenReturn(xmlFSDataInputStreamMock);
        Mockito.when(xmlFSDataInputStreamMock.getWrappedStream()).thenReturn(xmlStream);

        Mockito.when(fileSystemMock.open(new Path(TestData.DUMMY_HDFS_PATH_FIELD_META)))
                .thenReturn(fieldMetaFSDataInputStreamMock);
        Mockito.when(fieldMetaFSDataInputStreamMock.getWrappedStream()).thenReturn(fieldMetaStream);

        mapDriver = MapDriver.newMapDriver(new XmlParserMapper());
        mapDriver.getConfiguration().set(Constants.CONFIG_KEY_FIELD_META_FILE_PATH, TestData.DUMMY_HDFS_PATH_FIELD_META);
    }

    @After
    public void tearDown() throws IOException {
        xmlStream.close();
        fieldMetaStream.close();
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new Text("1"), new Text(TestData.DUMMY_HDFS_PATH_XML))
                .withOutput(new IntWritable(1), TestData.ARTICLE_INFO_MAP).runTest();
    }
}
