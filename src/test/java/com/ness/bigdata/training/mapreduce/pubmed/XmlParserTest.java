package com.ness.bigdata.training.mapreduce.pubmed;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.xpath.XPathExpressionException;

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
        String title = parser.evaluate("string(/article/front/article-meta/title-group/article-title)");
        assertThat(title, equalTo(TestData.ARTICLE_TITLE));
        String issnPPub = parser.evaluate("/article/front/journal-meta/issn[@pub-type='ppub']/text()");
        assertThat(issnPPub, equalTo(TestData.ARTICLE_ISSN_PUB));
        String publisherId = parser
                .evaluate("/article/front/article-meta/article-id[@pub-id-type='publisher-id']/text()");
        assertThat(publisherId, equalTo(TestData.ARTICLE_PUBLISHER_ID));
        String acceptedDate = parser.evaluate(
                "concat(/article/front/article-meta/history/date[@ date-type='accepted']/year/text(),'-',/article/front/article-meta/history/date[@ date-type='accepted']/month/text(),'-',/article/front/article-meta/history/date[@ date-type='accepted']/day/text())");
        assertThat(acceptedDate, equalTo(TestData.ARTICLE_DATE_ACCEPTED));
    }
}
