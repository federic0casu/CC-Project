package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.CustomInputSplit.CustomCombineFileInputFormat;
import it.unipi.dii.hadoop.Mapper.LetterCountMapper;
import it.unipi.dii.hadoop.Mapper.LetterFrequencyMapper;
import it.unipi.dii.hadoop.Reducer.LetterFrequencyReducer;
import it.unipi.dii.hadoop.Reducer.LetterCountReducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;


public class LetterFrequencyMain {
    private static final Configuration conf = new Configuration();
    private static final String countJobStatsPath = "/CC-Project/stats/count_job.stats";
    private static final String frequencyJobStatsPath = "/CC-Project/stats/frequency_job.stats";

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Usage: LetterFrequencyMain " +
                    "<input path> <output path countJob> <output path frequencyJob> " +
                    "<CustomInputSplit [1/0]> <#tasks per JVM> <#num Reducers> <#run>");
            System.exit(1);
        }

        String inputPath              = args[0];
        String countJobOutputPath     = args[1];
        String frequencyJobOutputPath = args[2];

        int CUSTOM_INPUT_SPLIT = Integer.parseInt(args[3]);
        int NUM_TASKS_JVM      = Integer.parseInt(args[4]);
        int NUM_REDUCERS       = Integer.parseInt(args[5]);
        int RUN                = Integer.parseInt(args[6]);

        // DEBUG
        System.out.println("########## MAP REDUCE BASED LETTER FREQUENCY ##########");
        System.out.println("You provided the following arguments:");
        System.out.println("\tinput path:              " + inputPath);
        System.out.println("\toutput path [count]:     " + countJobOutputPath);
        System.out.println("\toutput path [frequency]: " + frequencyJobOutputPath);
        System.out.println("\tCustomInputSplit [1/0]:  " + CUSTOM_INPUT_SPLIT);
        System.out.println("\tnumber of tasks per JVM: " + NUM_TASKS_JVM);
        System.out.println("\tnumber of Reducers:      " + NUM_REDUCERS);
        System.out.println("\trun:                     " + RUN);
        System.out.println("#######################################################");
        // DEBUG

        // Setting number of task per spawned JVM
        if (NUM_TASKS_JVM > 1)
            conf.set("mapred.job.reuse.jvm.num.tasks", Integer.toString(NUM_TASKS_JVM));

        // Count number of total letter job
        Job countJob = Job.getInstance(conf);
        countJob.setJarByClass(LetterFrequencyMain.class);
        countJob.setJobName("Letter Count Hadoop-based");

        countJob.setMapperClass(LetterCountMapper.class);
        countJob.setReducerClass(LetterCountReducer.class);

        // Setting CombineFileInputSplit
        if (CUSTOM_INPUT_SPLIT == 1)
            countJob.setInputFormatClass(CustomCombineFileInputFormat.class);

        // define mapper's output key-value
        countJob.setMapOutputKeyClass(Text.class);
        countJob.setMapOutputValueClass(IntWritable.class);

        // define reducer's output key-value
        countJob.setOutputKeyClass(Text.class);
        countJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(countJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(countJob, new Path(countJobOutputPath));

        // Record time to measure count Job performances
        long countJobStartTime = System.nanoTime();
        int exitStatus = countJob.waitForCompletion(true) ? 0 : 1;
        double countJobExecTime = (System.nanoTime() - countJobStartTime) / 1000000000.0;
        if (exitStatus == 1) {
            System.exit(1);
        }
        writeStats(countJobStatsPath, RUN, countJobExecTime, CUSTOM_INPUT_SPLIT, NUM_TASKS_JVM);

        // Load output of countJob from HDFS
        try {
            FileSystem fs = FileSystem.get(conf);
            Path outputPath = new Path(countJobOutputPath + "/part-r-00000");
            long letter_count = Utils.readLetterCountValue(fs, outputPath);
            conf.setLong("letter_count", letter_count);
        } catch (IOException e) {
            System.err.println("Error loading configuration file from HDFS: " + e.getMessage());
            System.exit(1);
        }

        // Letter frequency job
        Job frequencyJob = Job.getInstance(conf);
        frequencyJob.setJarByClass(LetterFrequencyMain.class);
        frequencyJob.setJobName("Letter Frequency analyzer Hadoop-based");

        // Setting CombineFileInputSplit
        if (CUSTOM_INPUT_SPLIT == 1)
            frequencyJob.setInputFormatClass(CustomCombineFileInputFormat.class);

        frequencyJob.setMapperClass(LetterFrequencyMapper.class);
        frequencyJob.setReducerClass(LetterFrequencyReducer.class);

        // define mapper's output key-value
        frequencyJob.setMapOutputKeyClass(Text.class);
        frequencyJob.setMapOutputValueClass(IntWritable.class);

        // define reducer's output key-value
        frequencyJob.setOutputKeyClass(Text.class);
        frequencyJob.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(frequencyJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(frequencyJob, new Path(frequencyJobOutputPath));

        // Record time to measure frequency Job performances
        long frequencyJobStartTime = System.nanoTime();
        exitStatus = frequencyJob.waitForCompletion(true) ? 0 : 1;
        double frequencyJobExecTime = (System.nanoTime() - frequencyJobStartTime) / 1000000000.0;
        writeStats(frequencyJobStatsPath, RUN, frequencyJobExecTime, CUSTOM_INPUT_SPLIT, NUM_TASKS_JVM);

        System.exit(exitStatus);
    }

    private static void writeStats(String logPath, int run, double time, int customInputSplit, int tasksJVM)
            throws IOException {
        try (FileSystem fs = FileSystem.get(conf)) {
            FSDataOutputStream out = fs.append(new Path(logPath));
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out));

            br.write(run + ",");
            br.write(time + ",");
            br.write(customInputSplit  + ",");
            br.write(tasksJVM + "\n");

            br.close();
        }
    }

}