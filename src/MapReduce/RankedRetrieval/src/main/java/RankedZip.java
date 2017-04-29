/**
 * Created by lihuaz on 4/22/17.
 */
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RankedZip {

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

    // part1------------------------------------------------------------------------
    public static class TFMapper extends Mapper<Text, BytesWritable, Text, Text> {

        private final Text one = new Text("1");
        private Text label = new Text();
        private int allWordCount = 0;
        private String fileName = "";

        @Override
        protected void map(Text key, BytesWritable value, Mapper<Text, BytesWritable, Text, Text>.Context context)
                throws IOException, InterruptedException {
            allWordCount = 0;
            fileName = key.toString();
            String content = new String( value.getBytes(), "UTF-8" );
            content = content.replaceAll("[^a-zA-Z]+", " ");
            StringTokenizer tokenizer = new StringTokenizer(content);
            while (tokenizer.hasMoreTokens()) {
                String currToken = tokenizer.nextToken().toLowerCase();
                if (reutersStopWords.contains(currToken)) {
                    continue;
                } else {
                    allWordCount++;
                    label.set(currToken + ":" + fileName);
                    context.write(label, one);
                }
            }
            context.write(new Text("!:" + fileName), new Text(String.valueOf(allWordCount)));
        }
    }

    public static class TFCombiner extends Reducer<Text, Text, Text, Text> {
        private int allWordCount = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            if (values == null) {
                return;
            }

            if(key.toString().startsWith("!")) {
                allWordCount = Integer.parseInt(values.iterator().next().toString());
                return;
            }

            int sumCount = 0;
            for (Text value : values) {
                sumCount += Integer.parseInt(value.toString());
            }

            double tf = 1.0 * sumCount / allWordCount;
            context.write(key, new Text(String.valueOf(tf)));
        }
    }

    public static class TFReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (values == null) {
                return;
            }

            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static class TFPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String fileName = key.toString().split(":")[1];
            return Math.abs((fileName.hashCode() * 127) % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(RankedZip.class);

        job.setMapperClass(TFMapper.class);
        job.setCombinerClass(TFCombiner.class);
        job.setReducerClass(TFReducer.class);

        job.setInputFormatClass(ZipFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        ZipFileInputFormat.setLenient(true);
        ZipFileInputFormat.setInputPaths(job, new Path("data/*.zip"));
        TextOutputFormat.setOutputPath(job, new Path("tf_zip"));

        job.waitForCompletion(true);
    }
}
