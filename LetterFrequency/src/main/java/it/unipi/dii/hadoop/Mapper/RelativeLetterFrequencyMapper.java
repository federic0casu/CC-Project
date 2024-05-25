package it.unipi.dii.hadoop.Mapper;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RelativeLetterFrequencyMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private int totalLetterOccurrences = 0;
    private static Map<String, Integer> relativeFrequencies;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        relativeFrequencies = new HashMap<>();
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");

        if (parts.length == 2) {
            relativeFrequencies.put(parts[0], Integer.parseInt(parts[1]));
        }

        totalLetterOccurrences += Integer.parseInt(parts[1]);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : relativeFrequencies.entrySet()) {
            double relativeFrequency = (double) entry.getValue() / totalLetterOccurrences;
            context.write(new Text(entry.getKey()), new DoubleWritable(relativeFrequency));
        }
    }
}