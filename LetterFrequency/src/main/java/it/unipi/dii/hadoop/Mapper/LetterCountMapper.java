package it.unipi.dii.hadoop.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;


public class LetterCountMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final static String TOTAL_COUNT = "letter_count";
    private final Text totalCountKey = new Text(TOTAL_COUNT);

    private Configuration conf;
    private String statsPath;
    private String ID;
    private long startTime;
    private int custom_input_split;
    private int num_reducers;
    private int dim_dataset;
    private int run;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        conf = context.getConfiguration();

        run = Integer.parseInt(conf.get("RUN"));
        num_reducers = Integer.parseInt(conf.get("NUM_REDUCERS"));
        dim_dataset = Integer.parseInt(conf.get("DIM_DATASET"));
        custom_input_split = Integer.parseInt(conf.get("CUSTOM_INPUT_SPLIT"));
        statsPath = conf.get("COUNT_MAPPERS_STATS");

        // Get TaskAttemptID for unique identifier generation (if needed)
        TaskAttemptID taskAttemptID = context.getTaskAttemptID();
        ID = taskAttemptID.toString() + "_" + run + "_" + num_reducers + "_" + dim_dataset + "MB";

        // DEBUG
        System.out.println("############ MAP REDUCE BASED LETTER FREQUENCY ############");
        System.out.println("LetterCountMapper::setup() >> " + ID);
        System.out.println("###########################################################");
        // DEBUG

        startTime = System.nanoTime();
    }

    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        char[] chars = value.toString().toLowerCase().toCharArray();

        long count = 0;
        for (char c : chars) {
            if (Character.isAlphabetic(c)) {
                count++;
            }
        }

        IntWritable countWritable = new IntWritable((int) count);
        context.write(totalCountKey, countWritable);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        double execTime = (System.nanoTime() - startTime) / 1_000_000_000.0;
        writeStats(execTime);
    }

    private void writeStats(double time)
            throws IOException {
        try (FileSystem fs = FileSystem.get(conf)) {
            FSDataOutputStream out = fs.append(new Path(statsPath));
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out));

            br.write(run + ",");
            br.write(time + ",");
            br.write(custom_input_split  + ",");
            br.write(num_reducers + ",");
            br.write(dim_dataset + "MB\n");

            br.close();
        }
    }
}
