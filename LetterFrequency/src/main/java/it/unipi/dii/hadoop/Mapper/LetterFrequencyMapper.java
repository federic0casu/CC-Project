package it.unipi.dii.hadoop.Mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static it.unipi.dii.hadoop.Utils.LETTERS;


public class LetterFrequencyMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private Map<Character, Integer> map;
    private static final String regex = "\\s+";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // Initialize map
        map = new HashMap<>();
        for(Character item: LETTERS) {
            map.put(item, 0);
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) {
        // Get the input split
        String[] words = value.toString().split(regex);

        for(String word: words) {
            char[] chars = word.toCharArray();
            for (int i = 0; i < word.length(); i++) {
                char tmp = Character.toLowerCase(chars[i]);
                if (LETTERS.contains(tmp))
                    map.put(tmp, map.get(tmp) + 1);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(Map.Entry<Character, Integer> entry: map.entrySet()) {
            context.write(
                    new Text(entry.getKey().toString()),
                    new IntWritable(entry.getValue())
            );
        }
    }
}
