package com.ness.bigdata.training.mapreduce.pubmed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.WritableComparable;

public class ArticleInfo implements WritableComparable<ArticleInfo> {
	private String filePath;
	private String articleTitle;
	private Long articlePublisherId;
	private String articleIssnPPub;
	private Long articleDateAccepted;

	public ArticleInfo(String filePath, String articleTitle, Long articlePublisherId, String articleIssnPPub,
			Long articleDateAccepted) {
		this.filePath = filePath;
		this.articleTitle = articleTitle;
		this.articlePublisherId = articlePublisherId;
		this.articleIssnPPub = articleIssnPPub;
		this.articleDateAccepted = articleDateAccepted;
	}

	public ArticleInfo() {
	}

	public String getFilePath() {
		return filePath;
	}

	public String getArticleTitle() {
		return articleTitle;
	}

	public Long getArticlePublisherId() {
		return articlePublisherId;
	}

	public String getArticleIssnPPub() {
		return articleIssnPPub;
	}

	public Long getArticleDateAccepted() {
		return articleDateAccepted;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(filePath);
		out.writeUTF(articleTitle);
		out.writeLong(articlePublisherId);
		out.writeUTF(articleIssnPPub);
		out.writeLong(articleDateAccepted);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		filePath = in.readUTF();
		articleTitle = in.readUTF();
		articlePublisherId = in.readLong();
		articleIssnPPub = in.readUTF();
		articleDateAccepted = in.readLong();
	}

	@Override
	public int compareTo(ArticleInfo other) {
		return filePath.compareTo(other.getArticleIssnPPub());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((articleDateAccepted == null) ? 0 : articleDateAccepted.hashCode());
		result = prime * result + ((articleIssnPPub == null) ? 0 : articleIssnPPub.hashCode());
		result = prime * result + ((articlePublisherId == null) ? 0 : articlePublisherId.hashCode());
		result = prime * result + ((articleTitle == null) ? 0 : articleTitle.hashCode());
		result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ArticleInfo other = (ArticleInfo) obj;
		if (articleDateAccepted == null) {
			if (other.articleDateAccepted != null)
				return false;
		} else if (!articleDateAccepted.equals(other.articleDateAccepted))
			return false;
		if (articleIssnPPub == null) {
			if (other.articleIssnPPub != null)
				return false;
		} else if (!articleIssnPPub.equals(other.articleIssnPPub))
			return false;
		if (articlePublisherId == null) {
			if (other.articlePublisherId != null)
				return false;
		} else if (!articlePublisherId.equals(other.articlePublisherId))
			return false;
		if (articleTitle == null) {
			if (other.articleTitle != null)
				return false;
		} else if (!articleTitle.equals(other.articleTitle))
			return false;
		if (filePath == null) {
			if (other.filePath != null)
				return false;
		} else if (!filePath.equals(other.filePath))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ArticleInfo [filePath=" + filePath + ", articleTitle=" + articleTitle + ", articlePublisherId="
				+ articlePublisherId + ", articleIssnPPub=" + articleIssnPPub + ", articleDateAccepted="
				+ articleDateAccepted + "]";
	}
	
	public GenericRecord toAvroGenericRecord() {
        GenericRecord genericRecord = new GenericData.Record(XmlParserJob.SCHEMA);
        genericRecord.put("filePath", filePath);
        genericRecord.put("articleTitle", articleTitle);
        genericRecord.put("articlePublisherId", articlePublisherId);
        genericRecord.put("articleIssnPPub", articleIssnPPub);
        genericRecord.put("articleDateAccepted", articleDateAccepted);
        return genericRecord;
	}
}
