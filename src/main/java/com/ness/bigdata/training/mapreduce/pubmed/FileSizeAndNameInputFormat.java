package com.ness.bigdata.training.mapreduce.pubmed;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author radu.almasan@ness.com
 */
public class FileSizeAndNameInputFormat extends FileInputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileSizeAndNameRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    /**
     * @author radu.almasan@ness.com
     */
    public static class FileSizeAndNameRecordReader extends RecordReader<LongWritable, Text> {

        /** The key. */
        private LongWritable key;
        /** The value. */
        private Text value;
        /** The file split. */
        private FileSplit fileSplit;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            fileSplit = (FileSplit) split;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (key == null) {
                key = new LongWritable(fileSplit.getLength());
                value = new Text(fileSplit.getPath().toString());
                return true;
            }

            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            System.out.println("getting current key = " + key);
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return value == null ? 0 : 1;
        }

        @Override
        public void close() throws IOException {
        }
    }
}
