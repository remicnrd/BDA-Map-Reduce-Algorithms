package cs.bigdata.Tfidf;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.lang.Iterable;

public class WordcountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException

    {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }

}