/**
 * Created by lihuaz on 2/19/17.
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

public class BiWordIndex {

    public static class TokenizerMapper
            extends Mapper<Text, Text, Text, Text> {

        private Text word = new Text();
        //private Text name = new Text();

        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {
            // only letters and don't apply stop words for biword indexing
            String valString = value.toString().replaceAll("[^a-zA-Z]+", " ");
            StringTokenizer itr = new StringTokenizer(valString);

            String prev = "";
            if (itr.hasMoreTokens()) {
                prev = itr.nextToken().toLowerCase();
            }
            String curr = "";

            while (itr.hasMoreTokens()) {
                curr = itr.nextToken().toLowerCase();
                StringBuilder sb = new StringBuilder(prev + " " + curr);
                word.set(sb.toString());
                prev = curr;
                // reduce output: <term, file name>
                context.write(word, key);
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
        Job job = Job.getInstance(conf, "Inverted Index Biword");

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setJarByClass(BiWordIndex.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

