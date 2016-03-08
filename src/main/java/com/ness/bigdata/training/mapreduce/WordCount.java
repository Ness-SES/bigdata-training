package com.ness.bigdata.training.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static class WordMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public static final String CONFIG_REGEX_KEY = "REGEX_KEY";

        public enum WordMapperEnum {
            NoWordValue;
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            Configuration config = context.getConfiguration();
            String regex = config.get(CONFIG_REGEX_KEY);
            while (itr.hasMoreTokens()) {
                String text = itr.nextToken();
                boolean b = null != regex && text.matches(regex);
                if ((null != text && text.matches("[0-9a-zA-Z]*")) || (null != regex && text.matches(regex))) {
                    word.set(text);
                    context.write(word, one);
                } else {
                    context.getCounter(WordMapperEnum.NoWordValue).increment(1);
                }
            }
        }
    }

    public static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public static enum WordReducerEnum {
            TOTAL_WORDS, UNIQUE_WORDS, DASH_WORDS, WITH_NUMBERS_WORDS, WITHOUT_NUMBERS_WORDS;
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.getCounter(WordReducerEnum.UNIQUE_WORDS).increment(1);
            context.getCounter(WordReducerEnum.TOTAL_WORDS).increment(sum);
            if (key.toString().contains("-")) {
                context.getCounter(WordReducerEnum.DASH_WORDS).increment(1);
            }
            if (key.toString().matches("(.*)[0-9]+(.*)")) {
                context.getCounter(WordReducerEnum.WITH_NUMBERS_WORDS).increment(1);
            } else {
                context.getCounter(WordReducerEnum.WITHOUT_NUMBERS_WORDS).increment(1);
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordMapper.class);
        job.setCombinerClass(WordReducer.class);
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

