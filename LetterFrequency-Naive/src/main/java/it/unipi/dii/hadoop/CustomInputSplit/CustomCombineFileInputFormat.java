package it.unipi.dii.hadoop.CustomInputSplit;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;


public class CustomCombineFileInputFormat extends CombineFileInputFormat<FileLineWritable, Text> {
    public CustomCombineFileInputFormat(){
        super();
        setMaxSplitSize(67108864); // 64 MB, default block size on hadoop
    }

    public RecordReader<FileLineWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException{
        return new CombineFileRecordReader<>(
                (CombineFileSplit) split,
                context,
                it.unipi.dii.hadoop.CustomInputSplit.CombineFileRecordReader.class
        );
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file){
        return false;
    }
}

