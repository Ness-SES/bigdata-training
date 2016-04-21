package com.ness.bigdata.training.mapreduce.pubmed;

import java.text.ParseException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

public class TestData {
    static final Schema AVRO_SCHEMA = SchemaBuilder.record("ArticleInfo").fields().name(Constants.FIELD_NAME_FILE_PATH)
            .type().stringType().noDefault().name("articleTitle").type().stringType().noDefault()
            .name("articlePublisherId").type().intType().noDefault().name("articleIssnPPub").type().stringType()
            .noDefault().name("articleDateAccepted").type().longType().noDefault().endRecord();

    public static final String DUMMY_HDFS_PATH_XML = "/user/ubuntu/datasets/pubmed/unzipped/unzipped.A-B/3_Biotech/3_Biotech_2011_Dec_13_1(4)_217-225.nxml";
    public static final String DUMMY_HDFS_PATH_FIELD_META = "/user/ubuntu/training/FieldMeta.json";

    public static final String XML = "src/test/resources/3_Biotech_2011_Dec_13_1(4)_217-225.xml";
    public static final String FIELD_META = "src/test/resources/FieldMeta.json";

    public static final String ARTICLE_TITLE = "Evaluation of indigenous Trichoderma isolates from Manipur as biocontrol agent against Pythium aphanidermatum on common beans";
    public static final int ARTICLE_PUBLISHER_ID = 27;
    public static final String ARTICLE_ISSN_PUB = "2190-572X";
    public static final long ARTICLE_DATE_ACCEPTED;
    static {
        try {
            ARTICLE_DATE_ACCEPTED = FieldMeta.YYYY_MM_DD.parse("2011-09-28").getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public final static MapWritable ARTICLE_INFO_MAP = new MapWritable();
    static {
        ARTICLE_INFO_MAP.put(new Text("filePath"), new Text(DUMMY_HDFS_PATH_XML));
        ARTICLE_INFO_MAP.put(new Text("articleTitle"), new Text(ARTICLE_TITLE));
        ARTICLE_INFO_MAP.put(new Text("articlePublisherId"), new IntWritable(ARTICLE_PUBLISHER_ID));
        ARTICLE_INFO_MAP.put(new Text("articleIssnPPub"), new Text(ARTICLE_ISSN_PUB));
        ARTICLE_INFO_MAP.put(new Text("articleDateAccepted"), new LongWritable(ARTICLE_DATE_ACCEPTED));
    }

    public final static GenericRecord ARTICLE_INFO_GENERIC_RECORD = new GenericData.Record(AVRO_SCHEMA);
    static {
        ARTICLE_INFO_GENERIC_RECORD.put("filePath", DUMMY_HDFS_PATH_XML);
        ARTICLE_INFO_GENERIC_RECORD.put("articleTitle", ARTICLE_TITLE);
        ARTICLE_INFO_GENERIC_RECORD.put("articlePublisherId", ARTICLE_PUBLISHER_ID);
        ARTICLE_INFO_GENERIC_RECORD.put("articleIssnPPub", ARTICLE_ISSN_PUB);
        ARTICLE_INFO_GENERIC_RECORD.put("articleDateAccepted", ARTICLE_DATE_ACCEPTED);
    }
}
