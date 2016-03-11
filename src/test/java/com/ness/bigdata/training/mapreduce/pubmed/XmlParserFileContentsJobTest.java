package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PrepareForTest(FileSystem.class)
@RunWith(PowerMockRunner.class)
public class XmlParserFileContentsJobTest {
	private MapReduceDriver<Object, Text, IntWritable, ArticleInfo, AvroKey<Integer>, AvroValue<ArticleInfo>> mapReduceDriver;

	@Before
	public void setup() throws IOException {
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(new XmlParserFileContentsMapper(),
				new XmlParserReducer());
		Configuration driverConfiguration = mapReduceDriver.getConfiguration();

		String[] ioSerializations = driverConfiguration.getStrings("io.serializations");
		String[] newIOSerializations = new String[ioSerializations.length + 1];
		System.arraycopy(ioSerializations, 0, newIOSerializations, 0, ioSerializations.length);
		newIOSerializations[newIOSerializations.length - 1] = AvroSerialization.class.getName();

		driverConfiguration.setStrings("io.serializations", newIOSerializations);
		driverConfiguration.set("avro.serialization.value.writer.schema", XmlParserJob.SCHEMA.toString(true));
		driverConfiguration.set("avro.serialization.key.writer.schema", Schema.create(Schema.Type.INT).toString(true));
	}

	@Test
	public void testMapReduce() throws IOException {
		String fileContents = FileUtils
				.readFileToString(new File("src/test/resources/3_Biotech_2011_Dec_13_1(4)_217-225.xml"));
		Calendar calendar = Calendar.getInstance();
		calendar.set(2011, 9 - 1, 28, 0, 0, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		ArticleInfo expectedArticleInfo = new ArticleInfo("",
				"Evaluation of indigenous Trichoderma isolates from Manipur as biocontrol agent against Pythium aphanidermatum on common beans",
				27L, "2190-572X", calendar.getTimeInMillis());
		mapReduceDriver.withInput(NullWritable.get(), new Text(fileContents))
				.withOutput(new AvroKey<Integer>(1), new AvroValue<ArticleInfo>(expectedArticleInfo)).runTest();
	}
}
