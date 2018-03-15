package cs.bigdata.Tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Tfidf extends Configured implements Tool {


    public int run(String[] args) throws Exception {

        if (args.length != 2) {

            System.out.println("Usage: [input] [output]");

            System.exit(-1);

        }

        // Définition des fichiers d'entrée et de sorties
        Path inputPath = new Path(args[0]);
        Path step1 = new Path("temp1");
        Path step2 = new Path("temp2");
        Path outputPath = new Path(args[1]);
        
     // Suppression du fichier de sortie s'il existe déjà
        FileSystem fs = FileSystem.newInstance(getConf());
        FileStatus[] stat = fs.listStatus(inputPath);
        if (fs.exists(step1)) {
        	fs.delete(step1, true);
        }
        if (fs.exists(step2)) {
        	fs.delete(step2, true);
        }
        if (fs.exists(outputPath)) {
        	fs.delete(outputPath, true);
        }
        fs.close();


        // WORDCOUNT : Création du job
        Job Wordcount = Job.getInstance(getConf());
        Wordcount.setJobName("Wordcount");

        // WORDCOUNT : On précise les classes MyProgram, Map et Reduce
        Wordcount.setJarByClass(Tfidf.class);
        Wordcount.setMapperClass(WordcountMap.class);
        Wordcount.setReducerClass(WordcountReduce.class);

        // WORDCOUNT : Définition des types clé/valeur
        Wordcount.setMapOutputKeyClass(Text.class);
        Wordcount.setMapOutputValueClass(IntWritable.class);
        Wordcount.setOutputKeyClass(Text.class);
        Wordcount.setOutputValueClass(IntWritable.class);
        		Wordcount.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");

        // WORDCOUNT : Définition des fichiers d'entrée et de sorties
        FileInputFormat.addInputPath(Wordcount, inputPath);
        FileOutputFormat.setOutputPath(Wordcount, step1);
        if (!Wordcount.waitForCompletion(true)) {
            System.exit(1);
        }


        // WORDDOC : Création du job
        Job Worddoc = Job.getInstance(getConf());
        Worddoc.setJobName("Worddoc");

        // WORDDOC : On précise les classes MyProgram, Map et Reduce
        Worddoc.setJarByClass(Tfidf.class);
        Worddoc.setMapperClass(WorddocMap.class);
        Worddoc.setReducerClass(WorddocReduce.class);

        // WORDDOC : Définition des types clé/valeur
        Worddoc.setMapOutputKeyClass(Text.class);
        Worddoc.setMapOutputValueClass(Text.class);
        Worddoc.setOutputKeyClass(Text.class);
        Worddoc.setOutputValueClass(Text.class);
        		Worddoc.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");

        // WORDDOC : Définition des fichiers d'entrée et de sorties
        FileInputFormat.addInputPath(Worddoc, step1);
        FileOutputFormat.setOutputPath(Worddoc, step2);
        if (!Worddoc.waitForCompletion(true)) {
            System.exit(1);
        }


        // DOCWORD : Création du job
        Job Docword = Job.getInstance(getConf());
        Docword.setJobName("Docword");

        // DOCWORD : On précise les classes MyProgram, Map et Reduce
        Docword.setJarByClass(Tfidf.class);
        Docword.setMapperClass(DocwordMap.class);
        Docword.setReducerClass(DocwordReduce.class);

        // DOCWORD : Définition des types clé/valeur
        Docword.setMapOutputKeyClass(Text.class);
        Docword.setMapOutputValueClass(Text.class);
        Docword.setOutputKeyClass(Text.class);
        Docword.setOutputValueClass(DoubleWritable.class);
        		Docword.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
        		Docword.setJobName(String.valueOf(stat.length));


        // DOCWORD : Définition des fichiers d'entrée et de sorties
        FileInputFormat.addInputPath(Docword, step2);
        FileOutputFormat.setOutputPath(Docword, outputPath);


        return Docword.waitForCompletion(true) ? 0: 1;
    }


    public static void main(String[] args) throws Exception {

        Tfidf TfidfDriver = new Tfidf();

        int res = ToolRunner.run(TfidfDriver, args);

        System.exit(res);

    }

}

