package cs.bigdata.Pagerank;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.lang.Iterable;

public class InputReduce extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

    @Override

    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException

    {
        String outList = new String();
        
        for (IntWritable val : values) {
            outList = outList + "," + val.toString();
        }
        
        outList = outList.substring(1);
        context.write(key, new Text(outList));
    }

}