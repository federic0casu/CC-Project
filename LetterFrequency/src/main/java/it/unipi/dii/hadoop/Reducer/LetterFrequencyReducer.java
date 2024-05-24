package it.unipi.dii.hadoop.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

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
