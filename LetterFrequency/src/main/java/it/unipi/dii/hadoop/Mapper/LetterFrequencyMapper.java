package it.unipi.dii.hadoop.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import static it.unipi.dii.hadoop.Utils.LETTERS;


public class LetterFrequencyMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private Map<Character, Integer> map;
    private Configuration conf;
    private String statsPath;
    private long startTime;
    private int custom_input_split;
    private int num_reducers;
    private int run;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // Initialize map
        map = new HashMap<>();
        for(Character item: LETTERS) {
            map.put(item, 0);
        }

        conf = context.getConfiguration();

        run = Integer.parseInt(conf.get("RUN"));
        num_reducers = Integer.parseInt(conf.get("NUM_REDUCERS"));
        custom_input_split = Integer.parseInt(conf.get("CUSTOM_INPUT_SPLIT"));
        statsPath = conf.get("FREQUENCY_MAPPERS_STATS");

        startTime = System.nanoTime();
    }

    @Override
    protected void map(Object key, Text value, Context context) {
        // Get the input split
        String[] words = value.toString().split("\\s+");

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

        double execTime = (System.nanoTime() - startTime) / 1_000_000_000.0;
        writeStats(execTime);
    }

    private void writeStats(double time)
            throws IOException {
        try (FileSystem fs = FileSystem.get(conf)) {
            FSDataOutputStream out = fs.append(new Path(statsPath));
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out));

            br.write(run + ",");
            br.write(time + ",");
            br.write(custom_input_split  + ",");
            br.write(num_reducers + "\n");

            br.close();
        }
    }
}
