package it.unipi.dii.hadoop.Mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class LetterCountMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final static String TOTAL_COUNT = "letter_count";
    private final Text totalCountKey = new Text(TOTAL_COUNT);

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
}
