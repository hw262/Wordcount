
package com.hanwang.cs643;

/**
 *
 * @author Han Wang
 */
import java.io.IOException;
import java.util.Arrays;
import static java.util.Collections.addAll;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.log4j.Logger;

public class WordCountRank extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new WordCount(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "wordcount");
        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private static final Pattern UNDESIRABLES = Pattern.compile("[(){},.;!+\"?<>%]");
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private long numRecords = 0;
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
        private String elements[] = { "education", "politics", "sports", "agriculture" }; 
        private HashSet<String> dict = new HashSet<String>(Arrays.asList(elements));

        public void map(LongWritable offset, Text lineText, Reducer.Context context)
                throws IOException, InterruptedException {
            String line = lineText.toString();
            Text currentWord = new Text();
            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                }
                String cleanWord = UNDESIRABLES.matcher(word.toString()).replaceAll("");
                if(dict.contains(cleanWord)) {
                  context.write(new Text(cleanWord), one);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, MapWritable, Text, Text> {
        
        private MapWritable result = new MapWritable();
        
        public void reduce(Text key, Iterable<MapWritable> values, Reducer.Context context)
                throws IOException, InterruptedException {
            for (MapWritable value : values) {
                addAll(value);
            }

          	Text fileName = new Text();
          	for(int i=0; i < 3; i++){
          	    int big=0;
          	    Iterator it= result.entrySet().iterator();
          	    while(it.hasNext()){
          	        java.util.Map.Entry<Text, IntWritable> entry=  (java.util.Map.Entry<Text, IntWritable>)it.next();
                    if(big < entry.getValue().get()) {
          		          big = entry.getValue().get();
          		          fileName = entry.getKey();
                    }
                }
                //logging to local logs of reducer
                //can check the log files from the UI web manager

                result.remove(fileName);
                context.write(key,new Text(fileName.toString() + ": " + big));
            }
        }
         private void addAll(MapWritable mapWritable) {
            Set<Writable> keys = mapWritable.keySet();
            
            for (Writable key : keys) {
                IntWritable fromCount = (IntWritable) mapWritable.get(key);
                if (result.containsKey(key)) {
                    IntWritable count = (IntWritable) result.get(key);
                    count.set(count.get() + fromCount.get());
                } else {
                    result.put(key, fromCount);
                }
            }
        }
    }
    
}

