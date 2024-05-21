package it.unipi.dii.hadoop.Mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class RelativeLetterFrequencyMapper
        extends Mapper<Object, Text, Text, DoubleWritable> {
    
    private int totalLetterOccurences = 0;
    private static Map relativeFrequencies;

        @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        relativeFrequencies = new HashMap<String, Integer>();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        String[] parts = value.toString().split("\t");
        
        if (parts.length == 2) {
            Text letter = new Text(parts[0]);
            IntWritable frequency = new IntWritable(Integer.parseInt(parts[1]));
            relativeFrequencies.put(parts[0],Integer.parseInt(parts[1]));
        }

        totalLetterOccurences += Integer.parseInt(parts[1]);

    }

     public void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<Map.Entry<String, Integer>> temp = relativeFrequencies.entrySet().iterator();

        while(temp.hasNext()) {
            Map.Entry<String, Integer> entry = temp.next();
            String keyVal = entry.getKey();
            Integer countVal = entry.getValue();
            double relativefreq = (double) countVal / totalLetterOccurences;
            context.write(new Text(keyVal), new DoubleWritable(relativefreq));
        }
    }
}



