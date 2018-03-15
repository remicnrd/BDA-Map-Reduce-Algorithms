package cs.bigdata.Pagerank;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

//import ecp.bigdata.lab1.ex4.pagerank.PageRank.CustomCounters;


import java.io.IOException;
import java.util.StringTokenizer;

public class IterReduce extends Reducer<IntWritable, Text, IntWritable, Text> {
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
//		long numNodes = context.getCounter(CustomCounters.NODECOUNTER).getValue();
		double newPR = new Double(.15 / (double) 75879);
		String linkedNodes = new String();
		
		for (Text val : values){
			String v = val.toString();
			if (!v.substring(0, 1).equals("#")){
				double PR = Double.parseDouble(v);
				newPR = newPR + .85 * PR;
			}
			else {
				linkedNodes = val.toString().substring(1);
			}
		}
		
		
		context.write(key, new Text("#" + linkedNodes));
		
		double numConn = linkedNodes.split(",").length;
		
		StringTokenizer tokenizer = new StringTokenizer(linkedNodes, ",");
		while (tokenizer.hasMoreTokens()) {
			int linkedNode = Integer.parseInt(tokenizer.nextToken());
			context.write(new IntWritable(linkedNode), new Text(String.valueOf(newPR / numConn)));
		}
	}
}