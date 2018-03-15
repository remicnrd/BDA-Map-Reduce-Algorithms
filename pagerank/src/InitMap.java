package cs.bigdata.Pagerank;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class InitMap extends Mapper<LongWritable, Text, IntWritable, Text> {
// Overriding of the map method
@Override
protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
    {
    	String[] in_outList = value.toString().split(";");
		int currentNode = Integer.parseInt(in_outList[0]);
		context.write(new IntWritable(currentNode), new Text("#" + in_outList[1]));
		
		int numConnections = in_outList[1].split(",").length;
		double pagerank = ( (double) 1 / (double) 75879) / numConnections ;
		
		StringTokenizer tokenizer = new StringTokenizer(in_outList[1], ",");
		while (tokenizer.hasMoreTokens()) {
			int linkedNode = Integer.parseInt(tokenizer.nextToken());
			context.write(new IntWritable(linkedNode), new Text(String.valueOf(pagerank)));
			
		}
    }
}