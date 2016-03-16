package com.ness.bigdata.training.mapreduce.pubmed.parquet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

public class AVROToParquetArrayWritable implements Writable {

	private Writable[] values;
	private String strSchema;

	public AVROToParquetArrayWritable() {
	}

	public AVROToParquetArrayWritable(Writable[] values, String strSchema) {
		this.values = values;
		this.strSchema = strSchema;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(strSchema);
		out.writeInt(values.length);
		for (int i = 0; i < values.length; i++) {
			out.writeUTF(values[i].getClass().getName());
			values[i].write(out);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		strSchema = in.readUTF();
		values = new Writable[in.readInt()];
		for (int i = 0; i < values.length; i++) {
			String className = in.readUTF();
			try {
				Class<? extends Writable> clazz = (Class<? extends Writable>) Class.forName(className);
				Writable value = WritableFactories.newInstance(clazz);
				value.readFields(in);
				values[i] = value;
			} catch (ClassNotFoundException e) {
			}
		}
	}

	public Writable[] get() {
		return values;
	}

	public void set(Writable[] values) {
		this.values = values;
	}

	public String getStrSchema() {
		return strSchema;
	}

	public void setStrSchema(String strSchema) {
		this.strSchema = strSchema;
	}

	@SuppressWarnings("unchecked")
	public Object toArray() {
		Object[] result = new Object[values.length];
		try {
			for (int i = 0; i < values.length; i++) {
				Class<? extends Writable> clazz = (Class<? extends Writable>) Class
						.forName(values[i].getClass().getName());
				result[i] = WritableFactories.newInstance(clazz);
			}
			for (int i = 0; i < values.length; i++) {
				Array.set(result, i, values[i]);
			}
		} catch (ClassNotFoundException e) {
		}
		return result;
	}

	public String[] toStrings() {
		String[] strings = new String[values.length];
		for (int i = 0; i < values.length; i++) {
			strings[i] = values[i].toString();
		}
		return strings;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		AVROToParquetArrayWritable other = (AVROToParquetArrayWritable) obj;
		Writable[] otherValues = other.get();
		if ((null == this.values && null != otherValues) || (null != this.values && null == otherValues)) {
			return false;
		}
		if (this.values.length != otherValues.length) {
			return false;
		}
		boolean retVal = true;
		for (int i = 0; i < this.values.length; i++) {
			if (false == valuesEquals(this.values[i], otherValues[i])) {
				retVal = false;
				break;
			}
		}
		return retVal;
	}

	private boolean valuesEquals(Writable obj1, Writable obj2) {
		if (obj1 == obj2) {
			return true;
		}
		if ((null == obj1 && null != obj2) || (null != obj1 && null == obj2)) {
			return false;
		}
		if (obj1.getClass() != obj2.getClass()) {
			return false;
		}
		return obj1.equals(obj2);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		for (Writable value : values) {
			result = prime * result + ((null == value) ? 0 : value.hashCode());
		}
		return result;
	}
}
