package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.io.Writable;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class XmlParser {
	private final static DocumentBuilder DOC_BUILDER;
	private final static XPath X_PATH;
	
	private final Document doc;

	static {
		try {
			X_PATH = XPathFactory.newInstance().newXPath();
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			ignoreDTD(docBuilderFactory);
			DOC_BUILDER = docBuilderFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			throw new RuntimeException(e);
		}
	}

    private static void ignoreDTD(DocumentBuilderFactory docBuilderFactory) throws ParserConfigurationException {
        docBuilderFactory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
    }

	public XmlParser(InputStream stream) {
		try {
			doc = DOC_BUILDER.parse(stream);
		} catch (SAXException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public Writable evaluate(FieldMeta fieldMeta) {
	    try {
	        XPathExpression xPathExpression = X_PATH.compile(fieldMeta.getXPath());
            Object value = xPathExpression.evaluate(doc, fieldMeta.getXPathType());
            return fieldMeta.castToWritable(value);
        } catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }
	}
}
