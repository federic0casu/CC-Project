package it.unipi.dii.hadoop.Reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LetterFrequencyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double intermediate_frequencies = 0;
        for (DoubleWritable val : values) {
            intermediate_frequencies += val.get();
        }
        context.write(key, new DoubleWritable(intermediate_frequencies));
    }
}
