package com.ness.bigdata.training.mapreduce.pubmed;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.xpath.XPathConstants;

import org.apache.avro.Schema;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FieldMeta {
    static final SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat("yyyy-MM-dd");

    enum Type {
        STRING, INT, DATE_YYYY_MM_DD
    };

    @SuppressWarnings({ "serial" })
    private static final Map<Type, QName> XPATH_TYPES = new HashMap<Type, QName>() {
        {
            put(Type.STRING, XPathConstants.STRING);
            put(Type.DATE_YYYY_MM_DD, XPathConstants.STRING);
            put(Type.INT, XPathConstants.NUMBER);
        }
    };

    @SuppressWarnings({ "serial" })
    private static final Map<Type, Schema> AVRO_TYPES = new HashMap<Type, Schema>() {
        {
            put(Type.STRING, Schema.create(Schema.Type.STRING));
            put(Type.DATE_YYYY_MM_DD, Schema.create(Schema.Type.LONG));
            put(Type.INT, Schema.create(Schema.Type.INT));
        }
    };

    private String xPath;
    private String name;
    private Type type;

    public FieldMeta(String name, String xPath, Type type) {
        this.name = name;
        this.xPath = xPath;
        this.type = type;
    }

    public FieldMeta() {
    }

    public String getXPath() {
        return xPath;
    }

    public void setXPath(String xPath) {
        this.xPath = xPath;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Schema getAvroType() {
        return AVRO_TYPES.get(type);
    }

    public QName getXPathType() {
        return XPATH_TYPES.get(type);
    }

    public Writable castToWritable(Object value) {
        try {
            switch (type) {
            case INT:
                return new IntWritable(((Number) value).intValue());
            case DATE_YYYY_MM_DD:
                Date date = YYYY_MM_DD.parse((String) value);
                return new LongWritable(date.getTime());
            default:
                return new Text(value.toString());
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public Object castFromWritable(Writable value) {
        switch (type) {
        case INT:
            return new Integer(((IntWritable) value).get());
        case DATE_YYYY_MM_DD:
            return new Long(((LongWritable)value).get());
        default:
            return new Text(value.toString());
        }
    }
}
