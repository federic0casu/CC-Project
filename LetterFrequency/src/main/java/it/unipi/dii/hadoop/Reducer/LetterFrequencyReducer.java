package it.unipi.dii.hadoop.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LetterFrequencyReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int occurrences = 0;
        for (IntWritable val : values) {
            occurrences += val.get();
        }
        context.write(key, new IntWritable(occurrences));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // TODO
    }
}
