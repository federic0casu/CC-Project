package it.unipi.dii.hadoop.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class LetterFrequencyReducer
        extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    private Configuration conf;
    private String statsPath;
    private long startTime;
    private long count;
    private int custom_input_split;
    private int num_reducers;
    private int dim_dataset;
    private int run;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();

        run = Integer.parseInt(conf.get("RUN"));
        count = Integer.parseInt(conf.get("LETTER_COUNT"));
        num_reducers = Integer.parseInt(conf.get("NUM_REDUCERS"));
        dim_dataset = Integer.parseInt(conf.get("DIM_DATASET"));
        custom_input_split = Integer.parseInt(conf.get("CUSTOM_INPUT_SPLIT"));

        statsPath = conf.get("FREQUENCY_REDUCERS_STATS");

        startTime = System.nanoTime();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int occurrences = 0;
        for (IntWritable val : values) {
            occurrences += val.get();
        }
        context.write(key, new DoubleWritable((double) occurrences / count));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        double execTime = (System.nanoTime() - startTime) / 1_000_000_000.0;
        writeStats(context, execTime);
    }

    private void writeStats(Context context, double time)
            throws IOException {
        // Get TaskAttemptID
        TaskAttemptID taskAttemptID = context.getTaskAttemptID();
        String filePath = statsPath + taskAttemptID.toString() + "_" + run +  ".csv";

        try (FileSystem fs = FileSystem.get(conf)) {
            // Use createNewFile to create a new file for each task attempt
            FSDataOutputStream out = fs.create(new Path(filePath));
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out));

            // Write .csv header
            br.write("run,time,custom-input-split,num-reducers,dim-dataset\n");

            // Write statistics data
            br.write(
                    run + "," + time + "," + custom_input_split + "," + num_reducers + "," + dim_dataset + "\n"
            );

            br.close();
        }
    }
}
