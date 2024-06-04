package it.unipi.dii.hadoop.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.io.DoubleWritable;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import static it.unipi.dii.hadoop.Utils.LETTERS;


public class LetterFrequencyMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private Map<Character, Double> map;
    private Configuration conf;
    private String statsPath;
    private long startTime;
    private int custom_input_split;
    private int num_reducers;
    private int dim_dataset;
    private int run;
    private final Text letter = new Text();
    private Integer count = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        conf = context.getConfiguration();

        count = Integer.parseInt(conf.get("LETTER_COUNT"));
        run = Integer.parseInt(conf.get("RUN"));
        num_reducers = Integer.parseInt(conf.get("NUM_REDUCERS"));
        dim_dataset = Integer.parseInt(conf.get("DIM_DATASET"));
        custom_input_split = Integer.parseInt(conf.get("CUSTOM_INPUT_SPLIT"));
        statsPath = conf.get("FREQUENCY_MAPPERS_STATS");
        map = new HashMap<>();
        for(Character item: LETTERS) {
            map.put(item, 0.0);
        }
        startTime = System.nanoTime();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        // Get the input split
        String[] words = value.toString().split("\\s+");

        for(String word: words) {
            char[] chars = word.toCharArray();
            for (int i = 0; i < word.length(); i++) {
                char tmp = Character.toLowerCase(chars[i]);
                if (LETTERS.contains(tmp)) {
                    map.put(tmp, map.get(tmp) + (Double)(1 / Double.valueOf(count)));
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for(Map.Entry<Character, Double> entry: map.entrySet()) {
            context.write(
                    new Text(entry.getKey().toString()),
                    new DoubleWritable(entry.getValue())
            );
        }
        double execTime = (System.nanoTime() - startTime) / 1_000_000_000.0;
        writeStats(context, execTime);
    }

    private void writeStats(Context context, double time)
            throws IOException {
        // Get TaskAttemptID
        TaskAttemptID taskAttemptID = context.getTaskAttemptID();
        String filePath = statsPath + taskAttemptID.toString() + "_" + run +  ".csv";

        try (FileSystem fs = FileSystem.get(conf)) {
            // Use createNewFile to create a new file for each task attempt
            FSDataOutputStream out = fs.create(new Path(filePath));
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out));

            // Write .csv header
            br.write("run,time,custom-input-split,num-reducers,dim-dataset\n");

            // Write statistics data
            br.write(
                    run + "," + time + "," + custom_input_split + "," + num_reducers + "," + dim_dataset + "\n"
            );

            br.close();
        }
    }
}
