package it.unipi.dii.hadoop.Mapper;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class RelativeLetterFrequencyMapper
        extends Mapper<Object, Text, Text, IntWritable> {
    
    private int totalLetterOccurences = 0;
    Map relativeFrequencies = new HashMap<String, Integer>();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        String[] parts = value.toString().split("\t");
        
        if (parts.length == 2) {
            Text letter = new Text(parts[0]);
            IntWritable frequency = new IntWritable(Integer.parseInt(parts[1]));
            relativeFrequencies.put(parts[0], Integer.parseInt(parts[1]));
        }

        totalLetterOccurences += Integer.parseInt(parts[1]);
    }

     public void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<Map.Entry<String, Integer>> temp = relativeFrequencies.entrySet().iterator();

        while(temp.hasNext()) {
            Map.Entry<String, Integer> entry = temp.next();
            String keyVal = entry.getKey()+"";
            Integer countVal = entry.getValue();

            context.write(new Text(keyVal), new DoubleWritable(countVal / totalLetterOccurences));
        }
    }
}
