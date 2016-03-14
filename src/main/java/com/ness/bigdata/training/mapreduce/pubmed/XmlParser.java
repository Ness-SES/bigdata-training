package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class XmlParser {
	private final static DocumentBuilder DOC_BUILDER;
	private final static XPathExpression XPATH_TITLE;
	private final static XPathExpression XPATH_ISSN_PPUB;
	private final static XPathExpression XPTH_PUBLISHER_ID;
	private final static XPathExpression XPATH_ACCEPTED_DAY;
	private final static XPathExpression XPATH_ACCEPTED_MONTH;
	private final static XPathExpression XPATH_ACCEPTED_YEAR;

	private final String title;
	private final String issnPPub;
	private final Long publisherId;
	private final Long acceptedDate;

	static {
		try {
			XPath xpath = XPathFactory.newInstance().newXPath();
			XPATH_TITLE = xpath.compile("string(/article/front/article-meta/title-group/article-title)");
			XPATH_ISSN_PPUB = xpath.compile("/article/front/journal-meta/issn[@pub-type='ppub']/text()");
			XPTH_PUBLISHER_ID = xpath
					.compile("/article/front/article-meta/article-id[@pub-id-type='publisher-id']/text()");
			XPATH_ACCEPTED_DAY = xpath
					.compile("/article/front/article-meta/history/date[@ date-type='accepted']/day/text()");
			XPATH_ACCEPTED_MONTH = xpath
					.compile("/article/front/article-meta/history/date[@ date-type='accepted']/month/text()");
			XPATH_ACCEPTED_YEAR = xpath
					.compile("/article/front/article-meta/history/date[@ date-type='accepted']/year/text()");
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			docBuilderFactory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			DOC_BUILDER = docBuilderFactory.newDocumentBuilder();
		} catch (ParserConfigurationException | XPathExpressionException e) {
			throw new RuntimeException(e);
		}
	}

	public XmlParser(InputStream stream) {
		try {
			Document doc = DOC_BUILDER.parse(stream);
			title = (String) XPATH_TITLE.evaluate(doc, XPathConstants.STRING);
			issnPPub = (String) XPATH_ISSN_PPUB.evaluate(doc, XPathConstants.STRING);
			publisherId = ((Double) XPTH_PUBLISHER_ID.evaluate(doc, XPathConstants.NUMBER)).longValue();
			double acceptedDay = (Double) XPATH_ACCEPTED_DAY.evaluate(doc, XPathConstants.NUMBER);
			double acceptedMonth = (Double) XPATH_ACCEPTED_MONTH.evaluate(doc, XPathConstants.NUMBER);
			double acceptedYear = (Double) XPATH_ACCEPTED_YEAR.evaluate(doc, XPathConstants.NUMBER);
			acceptedDate = toDate(acceptedDay, acceptedMonth, acceptedYear);
		} catch (SAXException | IOException | XPathExpressionException e) {
			throw new RuntimeException(e);
		}
	}

	private Long toDate(double day, double month, double year) {
		Calendar calendar = Calendar.getInstance();
		calendar.set((int) year, (int) (month - 1), (int) day, 0, 0, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return calendar.getTimeInMillis();
	}

	public String getIssnPPub() {
		return issnPPub;
	}

	public String getTitle() {
		return title;
	}

	public Long getPublisherId() {
		return publisherId;
	}

	public Long getAcceptedDate() {
		return acceptedDate;
	}
}
