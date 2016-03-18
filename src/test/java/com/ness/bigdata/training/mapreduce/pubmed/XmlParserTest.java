package com.ness.bigdata.training.mapreduce.pubmed;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.xpath.XPathExpressionException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

public class XmlParserTest {
    private InputStream stream;

    @Before
    public void setUp() throws IOException, XPathExpressionException, SAXException {
        stream = new FileInputStream(new File(TestData.XML));
    }

    @After
    public void tearDown() throws IOException {
        stream.close();
    }

    @Test
    public void testParse() throws XPathExpressionException, SAXException, IOException {
        XmlParser parser = new XmlParser(stream);
        
        FieldMeta fieldMeta = new FieldMeta("articleTitle", "string(/article/front/article-meta/title-group/article-title)", FieldMeta.Type.STRING); 
        String title = ((Text)parser.evaluate(fieldMeta)).toString();
        assertThat(title, equalTo(TestData.ARTICLE_TITLE));
        
        fieldMeta = new FieldMeta("articleIssnPPub", "/article/front/journal-meta/issn[@pub-type='ppub']/text()", FieldMeta.Type.STRING); 
        String issnPPub = ((Text)parser.evaluate(fieldMeta)).toString();
        assertThat(issnPPub, equalTo(TestData.ARTICLE_ISSN_PUB));
        
        fieldMeta = new FieldMeta("articlePublisherId", "/article/front/article-meta/article-id[@pub-id-type='publisher-id']/text()", FieldMeta.Type.INT); 
        int publisherId = ((IntWritable)parser.evaluate(fieldMeta)).get();
        assertThat(publisherId, equalTo(TestData.ARTICLE_PUBLISHER_ID));
        
        fieldMeta = new FieldMeta("articleDateAccepted", "concat(/article/front/article-meta/history/date[@ date-type='accepted']/year/text(),'-',substring(concat('0',/article/front/article-meta/history/date[@ date-type='accepted']/month/text()),string-length(/article/front/article-meta/history/date[@ date-type='accepted']/month/text())),'-',substring(concat('0',/article/front/article-meta/history/date[@ date-type='accepted']/day/text()),string-length(/article/front/article-meta/history/date[@ date-type='accepted']/day/text())))", FieldMeta.Type.DATE_YYYY_MM_DD);
        long acceptedDate = ((LongWritable)parser.evaluate(fieldMeta)).get();
        assertThat(acceptedDate, equalTo(TestData.ARTICLE_DATE_ACCEPTED));
    }
}
