package cs.bigdata.Tfidf;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class DocwordMap extends Mapper<LongWritable, Text, Text, Text> {
// Overriding of the map method
@Override
protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
    {
        String[] wdcp = value.toString().split(";");
		context.write(new Text(wdcp[0]), new Text(wdcp[1]+";"+wdcp[2]+";"+wdcp[3]));
    }
}