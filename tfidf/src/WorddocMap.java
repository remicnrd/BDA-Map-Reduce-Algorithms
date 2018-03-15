package cs.bigdata.Tfidf;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class WorddocMap extends Mapper<LongWritable, Text, Text, Text> {
// Overriding of the map method
@Override
protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
    {
        String[] wdc = value.toString().split(";");
		context.write(new Text(wdc[1]), new Text(wdc[0]+";"+wdc[2]));
    }
}