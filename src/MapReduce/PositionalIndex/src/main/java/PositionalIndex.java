/**
 * Created by lihuaz on 2/23/17.
 */

import java.io.InterruptedIOException;
import java.util.*;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PositionalIndex {
    public static class Map
        extends Mapper<Text, Text, Text, Text> {

        private static Set<String> reutersStopWords;
        static {
            reutersStopWords = new HashSet<String>();
            reutersStopWords.add("a");
            reutersStopWords.add("an");
            reutersStopWords.add("and");
            reutersStopWords.add("are");
            reutersStopWords.add("as");
            reutersStopWords.add("at");
            reutersStopWords.add("be");
            reutersStopWords.add("by");
            reutersStopWords.add("for");
            reutersStopWords.add("from");
            reutersStopWords.add("has");
            reutersStopWords.add("he");
            reutersStopWords.add("in");
            reutersStopWords.add("is");
            reutersStopWords.add("it");
            reutersStopWords.add("its");
            reutersStopWords.add("of");
            reutersStopWords.add("on");
            reutersStopWords.add("that");
            reutersStopWords.add("the");
            reutersStopWords.add("to");
            reutersStopWords.add("was");
            reutersStopWords.add("were");
            reutersStopWords.add("will");
            reutersStopWords.add("with");
        }

        private Text word = new Text();
        private Text name = new Text();

        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String valString = value.toString().replaceAll("[^a-zA-Z]+", " ");
            StringTokenizer itr = new StringTokenizer(valString);

//            FileSplit fileSplit = (FileSplit) context.getInputSplit();
//            String fileName = fileSplit.getPath().getName();

            int pos = 0;
            while (itr.hasMoreTokens()) {
                pos++;
                String curr = itr.nextToken().toLowerCase();
                if (reutersStopWords.contains(curr)) {
                    continue;
                } else {
                    word.set(curr);
                    name.set(key + " " + pos);
                    context.write(word, name);
                }
            }
        }
    }

    public static class Reduce
            extends Reducer<Text, Text, Text, Text> {
        public void reduce (Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
            for (Text val : values) {
                String[] valArray = val.toString().split(" ");
                if (!map.containsKey(valArray[0])) {
                    ArrayList<String> position = new ArrayList<String>();
                    position.add(valArray[1]);
                    map.put(valArray[0], position);
                } else {
                    map.get(valArray[0]).add(valArray[1]);
                }
            }
            context.write(key, new Text(map.toString()));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index Positional");

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setJarByClass(PositionalIndex.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
