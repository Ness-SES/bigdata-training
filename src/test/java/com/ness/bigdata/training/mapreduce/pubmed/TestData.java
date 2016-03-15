package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

public class TestData {
    static final Schema AVRO_SCHEMA; 
    static {
        try {
            InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("ArticleInfo.avsc");
            AVRO_SCHEMA = new Schema.Parser().parse(resourceAsStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static final String DUMMY_HDFS_PATH_AVSC2XPATH_PROPERTIES = "/user/ubuntu/training/avsc2xpath.properties";
    public static final String DUMMY_HDFS_PATH_XML = "/user/ubuntu/datasets/pubmed/unzipped/unzipped.A-B/3_Biotech/3_Biotech_2011_Dec_13_1(4)_217-225.nxml";

    public static final String AVSC2XPATH_PROPERTIES = "src/test/resources/avsc2xpath.properties";
    public static final String XML = "src/test/resources/3_Biotech_2011_Dec_13_1(4)_217-225.xml";

    public static final String ARTICLE_TITLE = "Evaluation of indigenous Trichoderma isolates from Manipur as biocontrol agent against Pythium aphanidermatum on common beans";
    public static final String ARTICLE_PUBLISHER_ID = "27";
    public static final String ARTICLE_ISSN_PUB = "2190-572X";
    public static final String ARTICLE_DATE_ACCEPTED = "2011-9-28";

    public final static MapWritable ARTICLE_INFO_MAP = new MapWritable();
    static {
        ARTICLE_INFO_MAP.put(new Text("filePath"), new Text(DUMMY_HDFS_PATH_XML));
        ARTICLE_INFO_MAP.put(new Text("articleTitle"), new Text(ARTICLE_TITLE));
        ARTICLE_INFO_MAP.put(new Text("articlePublisherId"), new Text(ARTICLE_PUBLISHER_ID));
        ARTICLE_INFO_MAP.put(new Text("articleIssnPPub"), new Text(ARTICLE_ISSN_PUB));
        ARTICLE_INFO_MAP.put(new Text("articleDateAccepted"), new Text(ARTICLE_DATE_ACCEPTED));
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
