package it.unipi.dii.hadoop.Mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static it.unipi.dii.hadoop.Utils.LETTERS;

public class LetterFrequencyMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private final DoubleWritable intermediateFrequency = new DoubleWritable();
    private final Text letter = new Text();

    @Override
    public void setup(Context context) {
        long count = Integer.parseInt(context.getConfiguration().get("LETTER_COUNT"));
        intermediateFrequency.set((double) 1 / count);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        char[] chars = value.toString().toLowerCase().toCharArray();

        for (char c : chars) {
            char lowerCaseChar = Character.toLowerCase(c);
            if (LETTERS.contains(lowerCaseChar)) {
                letter.set(String.valueOf(lowerCaseChar));
                context.write(letter, intermediateFrequency);
            }
        }
    }
}
