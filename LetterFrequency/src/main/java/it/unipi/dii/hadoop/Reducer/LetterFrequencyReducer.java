package it.unipi.dii.hadoop.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LetterFrequencyReducer
        extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    private int count;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        // Read a parameter
        count = Integer.parseInt(conf.get("letter_count"));
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
        // TODO
    }
}
