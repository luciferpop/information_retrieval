/**
 * Created by lihuaz on 2/17/17.
 */
import java.util.*;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.print.Doc;

public class InvertedIndex {

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

    public static class TokenizerMapper
            extends Mapper<Text, Text, Text, Text> {
        private Text word = new Text();

        /**
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         * For KeyValueTextInputFormat, the output of the RecordReader (which also is the input of Mapper) is <Text, Text> pair.
         * The key is the first String before '\t' (by default), the value is the rest of String after '\t'.
         */
        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (key == null || value == null) {
                return;
            }

            if (!key.toString().matches("[0-9]+")) {
                return;
            }

            // only letters and numbers are kept, and also only indexing lowercase tokens
            String valString = value.toString().replaceAll("[^a-zA-Z]+", " ");
            StringTokenizer itr = new StringTokenizer(valString);

            while (itr.hasMoreTokens()) {
                String currToken = itr.nextToken().toLowerCase();
                if (reutersStopWords.contains(currToken)) {
                    continue;
                } else {
                    word.set(currToken);
                    // reduce output: <term, file name>
                    // the file name is key which is the first String before '\t' in the input file
                    context.write(word, key);
                }
            }
        }
    }

    public static class InvertedIndexReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            HashSet<String> set = new HashSet<String>();
            for (Text val : values) {
                set.add(val.toString());
            }
            context.write(key, new Text(set.toString()));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index Uniword");

        // Default separator is '\t', change input file format from Text to KeyValueText
        // b/c handling large file is more efficient than small file for Hadoop
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
