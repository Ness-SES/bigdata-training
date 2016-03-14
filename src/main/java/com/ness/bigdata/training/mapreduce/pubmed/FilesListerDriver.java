package com.ness.bigdata.training.mapreduce.pubmed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.UUID;

/**
 * @author radu.almasan@ness.com
 */
public class FilesListerDriver extends Configured {

    /** The logger for this class. */
    private static final Logger L = LoggerFactory.getLogger(FilesListerDriver.class);
    /** The interval for writing count information. */
    private static final int LOG_SPIT_INTERVAL = 5000;

    /**
     * @param args expected 2 arguments: 1st - input folder, 2nd - non existing output folder
     *
     * @throws Exception if any problems occur
     */
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String extraArgs[] = new GenericOptionsParser(configuration, args).getRemainingArgs();
        configuration.set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

        FileSystem fileSystem = FileSystem.get(configuration);
        Path tempFile = new Path("/tmp/" + UUID.randomUUID().toString());

        if (fileSystem.exists(new Path(extraArgs[1]))) {
            throw new IllegalArgumentException("Output path must not exist: " + extraArgs[1]);
        }

        try {
            findFiles(fileSystem, new Path(extraArgs[0]), tempFile);
            runMrJob(configuration, tempFile, new Path(extraArgs[1]));

        } finally {
            if (fileSystem.exists(tempFile)) {
                fileSystem.delete(tempFile, false);
            }
        }
    }

    /**
     * Search for files in the given input folder and write them in the temp file.
     *
     * @param fileSystem  the file system to use
     * @param inputFolder the input folder where to seach for files
     * @param tempFile    the temp files where to write the found file names
     *
     * @throws IOException if any IO problems occur
     */
    private static void findFiles(FileSystem fileSystem, Path inputFolder, Path tempFile) throws IOException {
        L.info("Creating temp file + " + tempFile);
        try (FSDataOutputStream fsDataOutputStream = fileSystem.create(tempFile);
             OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fsDataOutputStream);
             BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter)) {

            L.info("Starting to look for files in " + inputFolder);

            long start = System.currentTimeMillis();
            int n = 0, total = 0;
            RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(inputFolder, true);
            while (files.hasNext()) {
                LocatedFileStatus fileStatus = files.next();
                bufferedWriter.write(String.format("%d\t%s", fileStatus.getLen(), fileStatus.getPath()));
                bufferedWriter.newLine();

                n++;
                total++;
                long now = System.currentTimeMillis();
                long duration = now - start;
                if (duration > LOG_SPIT_INTERVAL) {
                    L.info(String.format("Found %d / %d files in %d ms.", n, total, duration));
                    start = now;
                    n = 0;
                }
            }

            L.info("Done searching for files. Found in total " + total);
        }
    }

    /**
     * Sort the files by their size and write them to the output path.
     *
     * @param configuration the configuration
     * @param inputFile     the input file path
     * @param outputPath    the output folder
     *
     * @throws IOException            if any IO problems occur
     * @throws InterruptedException   if the job is interrupted
     * @throws ClassNotFoundException if used classes are not found
     */
    private static void runMrJob(Configuration configuration, Path inputFile, Path outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(FilesListerDriver.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(FilesListerMapper.class);

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }

    /**
     * @author radu.almasan@ness.com
     */
    public static class FilesListerMapper extends Mapper<Text, Text, LongWritable, Text> {

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            LongWritable fileSize = new LongWritable();
            fileSize.set(Long.parseLong(key.toString()));
            context.write(fileSize, value);
        }
    }
}
