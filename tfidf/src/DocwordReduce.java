package cs.bigdata.Tfidf;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.Iterable;
import java.util.ArrayList;

public class DocwordReduce extends Reducer<Text, Text, Text, DoubleWritable> {

    @Override

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException

    {
    	double nbDocs = Integer.parseInt(context.getJobName());
        String word = key.toString();
        ArrayList<String> contents = new ArrayList<String>();
        double docsPerWord = 0;
        
        for (Text val : values) {
            contents.add(val.toString());
            docsPerWord++;
        }
        
        for (int i = 0; i < contents.size(); i++) {
            String[] dwpd = contents.get(i).split(";");
            String wd = word+";"+dwpd[0];
            double wc = Integer.parseInt(dwpd[1]);
            double wpd = Integer.parseInt(dwpd[2]);
            double tfidf = (wc/wpd)*Math.log(nbDocs / docsPerWord);
            
            context.write(new Text(wd), new DoubleWritable(tfidf));
        }
    }

}