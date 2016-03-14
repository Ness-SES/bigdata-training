package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;

class XmlParserTestUtils {

    private XmlParserTestUtils() {
    }

    static Schema getArticleInfoSchema() throws IOException {
        InputStream resourceAsStream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("ArticleInfo.avsc");
        return new Schema.Parser().parse(resourceAsStream);
    }
}
