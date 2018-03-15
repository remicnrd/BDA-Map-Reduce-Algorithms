package cs.bigdata.Pagerank;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class InputMap extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
// Overriding of the map method
@Override
protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
    {
    	String line = value.toString();
		if (!line.substring(0, 1).equals("#")){
			String[] in_out = line.split("\t");
			int in = Integer.parseInt(in_out[0]);
			int out = Integer.parseInt(in_out[1]);
			
			context.write(new IntWritable(in), new IntWritable(out));
			
		}
    }
}