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
    private static final String jobStatsPath              = "/CC-Project/stats/job/";
    private static final String countMapperStatsPath      = "/CC-Project/stats/count_mappers/";
    private static final String countReducerStatsPath     = "/CC-Project/stats/count_reducers/";
    private static final String frequencyMapperStatsPath  = "/CC-Project/stats/frequency_mappers/";
    private static final String frequencyReducerStatsPath = "/CC-Project/stats/frequency_reducers/";
    private static String INPUT_PATH;
    private static String COUNT_JOB_OUTPUT;
    private static String FREQUENCY_JOB_OUTPUT;
    private static int CUSTOM_INPUT_SPLIT;
    private static int NUM_REDUCERS;
    private static int DIM_DATASET;
    private static int RUN;

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Usage: LetterFrequencyMain " +
                    "<input path> <output path countJob> <output path frequencyJob> " +
                    "<CustomInputSplit [1/0]> <#num Reducers> <#run> <#sub-dirs>");
            System.exit(1);
        }

        INPUT_PATH           = args[0];
        COUNT_JOB_OUTPUT     = args[1];
        FREQUENCY_JOB_OUTPUT = args[2];
        CUSTOM_INPUT_SPLIT   = Integer.parseInt(args[3]);
        NUM_REDUCERS         = Integer.parseInt(args[4]);
        RUN                  = Integer.parseInt(args[5]);
        DIM_DATASET          = Integer.parseInt(args[6]);

        if (CUSTOM_INPUT_SPLIT != 0 && CUSTOM_INPUT_SPLIT != 1) {
            System.err.println("CustomInputSplit must be 0 or 1 (value :" + CUSTOM_INPUT_SPLIT + ")");
            System.exit(1);
        }

        if (NUM_REDUCERS < 1) {
            System.err.println("Number of Reducers must be greater than 0 (value: " + NUM_REDUCERS + ")");
            System.exit(1);
        }

        if (DIM_DATASET < 1 || DIM_DATASET > 6) {
            System.err.println("#sub-dirs must be between 1 and 6 (value: " + DIM_DATASET + ")");
            System.exit(1);
        }

        Configuration conf = setConfiguration();

        // DEBUG
        printArguments();
        // DEBUG

        // //////////////////// STAGE 1 ////////////////////
        // Count number of total letter job
        Job countJob = Job.getInstance(conf);
        countJob.setJarByClass(LetterFrequencyMain.class);
        countJob.setJobName("Letter Count Hadoop-based");

        // define mapper and reduce class
        countJob.setMapperClass(LetterCountMapper.class);
        countJob.setReducerClass(LetterCountReducer.class);

        // Setting CombineFileInputSplit
        if (CUSTOM_INPUT_SPLIT == 1) {
            countJob.setInputFormatClass(CustomCombineFileInputFormat.class);
        }

        // define mapper's output key-value
        countJob.setMapOutputKeyClass(Text.class);
        countJob.setMapOutputValueClass(IntWritable.class);

        // define reducer's output key-value
        countJob.setOutputKeyClass(Text.class);
        countJob.setOutputValueClass(IntWritable.class);

        // DIM_DATASET can be in [1,6]
        for (int i = 1; i <= DIM_DATASET; i++)
            FileInputFormat.addInputPath(countJob, new Path(INPUT_PATH + "/" + i));

        FileOutputFormat.setOutputPath(countJob, new Path(COUNT_JOB_OUTPUT));

        // Record time to measure Job performances
        long countJobStartTime = System.nanoTime();
        int exitStatus = countJob.waitForCompletion(true) ? 0 : 1;
        double countJobExecTime = (System.nanoTime() - countJobStartTime) / 1000000000.0;

        if (exitStatus == 1) {
            System.exit(1);
        }

        // Load output of countJob from HDFS
        loadLetterCountFromHDFS(conf, COUNT_JOB_OUTPUT);


        // //////////////////// STAGE 2 ////////////////////
        // Letter frequency job
        Job frequencyJob = Job.getInstance(conf);
        frequencyJob.setJarByClass(LetterFrequencyMain.class);
        frequencyJob.setJobName("Letter Frequency analyzer Hadoop-based");

        frequencyJob.setMapperClass(LetterFrequencyMapper.class);
        frequencyJob.setReducerClass(LetterFrequencyReducer.class);

        // Setting CombineFileInputSplit
        if (CUSTOM_INPUT_SPLIT == 1) {
            frequencyJob.setInputFormatClass(CustomCombineFileInputFormat.class);
        }

        // define mapper's output key-value
        frequencyJob.setMapOutputKeyClass(Text.class);
        frequencyJob.setMapOutputValueClass(DoubleWritable.class);

        // define reducer's output key-value
        frequencyJob.setOutputKeyClass(Text.class);
        frequencyJob.setOutputValueClass(DoubleWritable.class);

        // DIM_DATASET can be 1, 2, 4, or 6
        for (int i = 1; i <= DIM_DATASET; i++)
            FileInputFormat.addInputPath(frequencyJob, new Path(INPUT_PATH + "/" + i));

        FileOutputFormat.setOutputPath(frequencyJob, new Path(FREQUENCY_JOB_OUTPUT));

        // Record time to Job performances
        double frequencyJobStartTime = System.nanoTime();
        exitStatus = frequencyJob.waitForCompletion(true) ? 0 : 1;
        double frequencyJobExecTime = (System.nanoTime() - frequencyJobStartTime) / 1000000000.0;

        writeStats(conf, countJobExecTime + frequencyJobExecTime);

        System.exit(exitStatus);
    }

    private static Configuration setConfiguration() {
        Configuration conf = new Configuration();

        conf.setLong("RUN", RUN);
        conf.setLong("NUM_REDUCERS", NUM_REDUCERS);
        conf.setLong("DIM_DATASET", (DIM_DATASET * 200L));
        conf.setLong("CUSTOM_INPUT_SPLIT", CUSTOM_INPUT_SPLIT);
        conf.setStrings("COUNT_MAPPERS_STATS", countMapperStatsPath);
        conf.setStrings("COUNT_REDUCERS_STATS", countReducerStatsPath);
        conf.setStrings("FREQUENCY_MAPPERS_STATS", frequencyMapperStatsPath);
        conf.setStrings("FREQUENCY_REDUCERS_STATS", frequencyReducerStatsPath);

        return conf;
    }

    private static void loadLetterCountFromHDFS(Configuration conf, String outputPath) throws IOException {
        try (FileSystem fs = FileSystem.get(conf)) {
            Path path = new Path(outputPath + "/part-r-00000");
            long count = Utils.readLetterCountValue(fs, path);
            conf.setLong("LETTER_COUNT", count);
        } catch (IOException e) {
            System.err.println("Error loading letter count from HDFS: " + e.getMessage());
            throw e;
        }
    }

    private static void writeStats(Configuration conf, double time)
            throws IOException {
        String filePathName = jobStatsPath + (DIM_DATASET * 200) + "MB.csv";
        Path filePath = new Path(filePathName);

        try (FileSystem fs = FileSystem.get(conf)) {
            if (!fs.exists(filePath)) {
                if (!fs.createNewFile(filePath))
                    throw new IOException("Could not create file " + filePathName);

                FSDataOutputStream out = fs.append(filePath);
                BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out));

                // Write .csv header
                br.write("run,time,custom-input-split,num-reducers,dim-dataset\n");

                // Write statistics data
                br.write(
                        RUN + "," + time + "," + CUSTOM_INPUT_SPLIT + "," + NUM_REDUCERS + "," + (DIM_DATASET * 200) + "\n"
                );

                br.close();
            } else {
                FSDataOutputStream out = fs.append(filePath);
                BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out));

                // Write statistics data
                br.write(
                        RUN + "," + time + "," + CUSTOM_INPUT_SPLIT + "," + NUM_REDUCERS + "," + (DIM_DATASET * 200) + "\n"
                );

                br.close();
            }
        }
    }

    private static void printArguments() {
        System.out.println("############ MAP REDUCE BASED LETTER FREQUENCY ############");
        System.out.println("You provided the following arguments:");
        System.out.println("\tinput path:              " + INPUT_PATH);
        System.out.println("\toutput path [count]:     " + COUNT_JOB_OUTPUT);
        System.out.println("\toutput path [frequency]: " + FREQUENCY_JOB_OUTPUT);
        System.out.println("\tCustomInputSplit [1/0]:  " + CUSTOM_INPUT_SPLIT);
        System.out.println("\tnumber of Reducers:      " + NUM_REDUCERS);
        System.out.println("\tdataset [MB]:            " + DIM_DATASET * 200);
        System.out.println("\trun:                     " + RUN);
        System.out.println("###########################################################");
    }

}