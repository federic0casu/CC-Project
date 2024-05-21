package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.CustomInputSplit.CustomCombineFileInputFormat;
import it.unipi.dii.hadoop.Mapper.LetterCountMapper;
import it.unipi.dii.hadoop.Mapper.LetterFrequencyMapper;
import it.unipi.dii.hadoop.Reducer.LetterFrequencyReducer;
import it.unipi.dii.hadoop.Reducer.LetterCountReducer;
import it.unipi.dii.hadoop.Mapper.RelativeLetterFrequencyMapper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
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
        String frequencyJobOutputPath     = args[1];
        String relativeFrequencyJobOutputPath = args[2];

        int CUSTOM_INPUT_SPLIT = Integer.parseInt(args[3]);
        int NUM_TASKS_JVM      = Integer.parseInt(args[4]);
        int NUM_REDUCERS       = Integer.parseInt(args[5]);
        int RUN                = Integer.parseInt(args[6]);

        // DEBUG
        System.out.println("########## MAP REDUCE BASED LETTER FREQUENCY ##########");
        System.out.println("You provided the following arguments:");
        System.out.println("\tinput path:              " + inputPath);
        System.out.println("\toutput path [count]:     " + frequencyJobOutputPath);
        System.out.println("\toutput path [frequency]: " + relativeFrequencyJobOutputPath);
        System.out.println("\tCustomInputSplit [1/0]:  " + CUSTOM_INPUT_SPLIT);
        System.out.println("\tnumber of tasks per JVM: " + NUM_TASKS_JVM);
        System.out.println("\tnumber of Reducers:      " + NUM_REDUCERS);
        System.out.println("\trun:                     " + RUN);
        System.out.println("#######################################################");
        // DEBUG

        // Setting number of task per spawned JVM
        if (NUM_TASKS_JVM > 1)
            conf.set("mapred.job.reuse.jvm.num.tasks", Integer.toString(NUM_TASKS_JVM));



        double startTime = System.nanoTime();

        // Count number of total letter job
        Job frequencyJob = Job.getInstance(conf);
        frequencyJob.setJarByClass(LetterFrequencyMain.class);
        frequencyJob.setJobName("Per Letter frequency");

        frequencyJob.setMapperClass(LetterFrequencyMapper.class);
        frequencyJob.setReducerClass(LetterFrequencyReducer.class);

        // Setting CombineFileInputSplit
        if (CUSTOM_INPUT_SPLIT == 1)
            frequencyJob.setInputFormatClass(CustomCombineFileInputFormat.class);

        // define mapper's output key-value
        frequencyJob.setMapOutputKeyClass(Text.class);
        frequencyJob.setMapOutputValueClass(IntWritable.class);

        // define reducer's output key-value
        frequencyJob.setOutputKeyClass(Text.class);
        frequencyJob.setOutputValueClass(IntWritable.class);

        

        FileInputFormat.addInputPath(frequencyJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(frequencyJob, new Path(frequencyJobOutputPath));

        int exitStatus = frequencyJob.waitForCompletion(true) ? 0 : 1;
        if (exitStatus == 1) {
            System.exit(1);
        }

        // ///////////////////// JOB 2 /////////////////////////////
        // Letter frequency job
        Job relativeFrequencyJob = Job.getInstance(conf);
        relativeFrequencyJob.setJarByClass(LetterFrequencyMain.class);
        relativeFrequencyJob.setJobName("Relative Letter Frequency analyzer Hadoop-based");

        relativeFrequencyJob.setMapperClass(RelativeLetterFrequencyMapper.class);

        /* Using identity reducer */
        relativeFrequencyJob.setReducerClass(Reducer.class);

        // define mapper's output key-value
        relativeFrequencyJob.setMapOutputKeyClass(Text.class);
        relativeFrequencyJob.setMapOutputValueClass(DoubleWritable.class);

        // define reducer's output key-value
        relativeFrequencyJob.setOutputKeyClass(Text.class);
        relativeFrequencyJob.setOutputValueClass(DoubleWritable.class);

        // The second job uses the output of the first job as its input
        FileInputFormat.addInputPath(relativeFrequencyJob, new Path(frequencyJobOutputPath));
        FileOutputFormat.setOutputPath(relativeFrequencyJob, new Path(relativeFrequencyJobOutputPath));

        exitStatus = relativeFrequencyJob.waitForCompletion(true) ? 0 : 1;
        
        double execTime =(System.nanoTime() - startTime) / 1000000000; 
        
        System.exit(exitStatus);
    }
}
