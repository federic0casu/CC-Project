package it.unipi.dii.hadoop.Mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static it.unipi.dii.hadoop.Utils.LETTERS;

public class LetterCountMapper extends Mapper<Object, Text, Text, IntWritable>  {
    private final Text countKey = new Text("COUNT");

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        char[] chars = value.toString().toLowerCase().toCharArray();

        long count = 0;
        for (char c : chars) {
            if (LETTERS.contains(c)) {
                count++;
            }
        }

        IntWritable countWritable = new IntWritable((int) count);
        context.write(countKey, countWritable);
    }
}
