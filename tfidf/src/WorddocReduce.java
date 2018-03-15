package cs.bigdata.Tfidf;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.Iterable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class WorddocReduce extends Reducer<Text, Text, Text, Text> {

    @Override

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException

    {
        String doc = key.toString();
        Map<String, String> WordcountMap = new HashMap<String, String>();
        int worddoc = 0;
        
        for (Text val : values) {
            String[] wc = val.toString().split(";");
            WordcountMap.put(wc[0], wc[1]);
            worddoc += Integer.parseInt(wc[1]);
        }
        
        Set<Entry<String, String>> setMap = WordcountMap.entrySet();
        Iterator<Entry<String, String>> iter = setMap.iterator();
        
        while(iter.hasNext()){
            
            Entry<String, String> ent = iter.next();
            String wd = ent.getKey()+";"+doc;
            String wcWpd = ent.getValue()+";"+worddoc;
            
            context.write(new Text(wd), new Text(wcWpd));
        }
    }

}