package cs.bigdata.Tfidf;



import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordcountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable ONE = new IntWritable(1);
	private Text word = new Text();
	
// Overriding of the map method
@Override
protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
    {
        FileSplit split = (FileSplit) context.getInputSplit();
		String filename = split.getPath().getName().toString();
		String line = value.toString().toLowerCase();
		StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r\f,.:;?![]{}'\"()&<>~_-#$*^%/@\\`=+|");

		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			String wordDoc = word.toString() + ";" + filename;
			context.write(new Text(wordDoc), ONE);
		}
    }
}