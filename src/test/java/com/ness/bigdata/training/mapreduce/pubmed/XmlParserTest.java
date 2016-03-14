package com.ness.bigdata.training.mapreduce.pubmed;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;

import javax.xml.xpath.XPathExpressionException;

import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

public class XmlParserTest {
    private InputStream stream;

    @Before
    public void setUp() throws IOException, XPathExpressionException, SAXException {
        stream = new FileInputStream(new File("src/test/resources/3_Biotech_2011_Dec_13_1(4)_217-225.xml"));
    }

    @Test
    public void testParse() throws XPathExpressionException, SAXException, IOException {
        XmlParser parser = new XmlParser(stream);
        assertThat(parser.getTitle(), equalTo(
                "Evaluation of indigenous Trichoderma isolates from Manipur as biocontrol agent against Pythium aphanidermatum on common beans"));
        assertThat(parser.getIssnPPub(), equalTo("2190-572X"));
        assertThat(parser.getPublisherId(), equalTo(27L));
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(parser.getAcceptedDate());
        assertThat(calendar.get(Calendar.DAY_OF_MONTH), equalTo(28));
        assertThat(calendar.get(Calendar.MONTH) + 1, equalTo(9));
        assertThat(calendar.get(Calendar.YEAR), equalTo(2011));
    }
}
