import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by lihuaz on 4/22/17.
 */
public class KVTf {
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
    public static class TFMapper extends Mapper<Text, Text, Text, Text> {

        private final Text one = new Text("1");
        private Text label = new Text();
        private int allWordCount = 0;
        private String fileName = "";

        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (key == null || value == null) {
                return;
            }
            allWordCount = 0;
            fileName = key.toString();
            if (!fileName.matches("[0-9]+")) {
                return;
            }
            String valString = value.toString().replaceAll("[^a-zA-Z]+", " ");
            StringTokenizer tokenizer = new StringTokenizer(valString);
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

    public static class TFReducer extends Reducer<Text, Text, Text, Text> {
        private static HashMap<String, Integer> map;

        static {
            map = new HashMap<String, Integer>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            if (values == null) {
                return;
            }

            String fileName = key.toString().replaceAll("\\D", "");

            if (key.toString().startsWith("!")) {
                map.put(fileName, Integer.parseInt(values.iterator().next().toString()));
                return;
            }

            int sumCount = 0;
            for (Text value : values) {
                sumCount += Integer.parseInt(value.toString());
            }

            double tf = 0;
            tf = 1.0 * sumCount / map.get(fileName);

            context.write(key, new Text(String.valueOf(tf)));
        }
    }

    public static class TFPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String fileName = key.toString().split(":")[1];
            return Math.abs((fileName.hashCode() * 127) % numPartitions);
        }
    }

    // part2-----------------------------------------------------
    public static class IDFMapper extends Mapper<Object, Text, Text, Text> {

        private final Text one = new Text("1");
        private Text label = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            label.set(tokenizer.nextToken().split(":")[0]);
            context.write(label, one);
        }
    }

    public static class IDFReducer extends Reducer<Text, Text, Text, Text> {

        private Text label = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            if (values == null) {
                return;
            }

            int fileCount = 0;
            for (Text value : values) {
                fileCount += Integer.parseInt(value.toString());
            }

            label.set(key.toString() + ":" + "!");
            double idfValue = Math.log10((4 - fileCount + 0.5) / (fileCount + 0.5));

            context.write(label, new Text(String.valueOf(idfValue)));
        }
    }

    public static class TF_IDFMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            context.write(new Text(tokenizer.nextToken()), new Text(tokenizer.nextToken()));
        }
    }

    public static class TF_IDFReducer extends Reducer<Text, Text, Text, Text> {

        private double keywordIDF = 0.0d;
        private Text value = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (values == null) {
                return;
            }

            if (key.toString().split(":")[1].startsWith("!")) {
                keywordIDF = Double.parseDouble(values.iterator().next().toString());
                return;
            }

            value.set(String.valueOf(Double.parseDouble(values.iterator().next().toString()) * keywordIDF));

            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {
        // part1----------------------------------------
        Configuration conf1 = new Configuration();
        System.out.println("Calculating TF.");

        Job job1 = Job.getInstance(conf1, "TF");
        job1.setJarByClass(KVTf.class);

        job1.setInputFormatClass(KeyValueTextInputFormat.class);

        job1.setMapperClass(TFMapper.class);
        job1.setReducerClass(TFReducer.class);
        job1.setPartitionerClass(TFPartitioner.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

        // part2----------------------------------------
        Configuration conf2 = new Configuration();
        System.out.println("Calculating IDF.");
        Job job2 = Job.getInstance(conf2, "IDF");
        job2.setJarByClass(KVTf.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(IDFMapper.class);
        job2.setReducerClass(IDFReducer.class);

        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);

        // part3----------------------------------------
        Configuration conf3 = new Configuration();
        System.out.println("Calculating TFIDF.");
        Job job3 = Job.getInstance(conf3);
        job3.setJobName("TF_IDF");
        job3.setJarByClass(KVTf.class);

        job3.setMapperClass(TF_IDFMapper.class);
        job3.setReducerClass(TF_IDFReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(args[1]));
        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        job3.waitForCompletion(true);
    }
}