import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RankedRetrieval {

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
    public static class TFMapper extends Mapper<Object, Text, Text, Text> {

        private final Text one = new Text("1");
        private Text label = new Text();
        private int allWordCount = 0;
        private String fileName = "";

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            fileName = getInputSplitFileName(context.getInputSplit());
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String valString = value.toString().replaceAll("[^a-zA-Z]+", " ");
            StringTokenizer tokenizer = new StringTokenizer(valString);
            while (tokenizer.hasMoreTokens()) {
                String currToken = tokenizer.nextToken().toLowerCase();
                if (reutersStopWords.contains(currToken)) {
                    continue;
                } else {
                    allWordCount++;
                    //label.set(String.join(":", currToken, fileName));
                    label.set(currToken + ":" + fileName);
                    context.write(label, one);
                }
            }
        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            context.write(new Text("!:" + fileName), new Text(String.valueOf(allWordCount)));
        }

        private String getInputSplitFileName(InputSplit inputSplit) {
            String fileFullName = ((FileSplit)inputSplit).getPath().toString();
            String[] nameSegments = fileFullName.split("/");
            return nameSegments[nameSegments.length - 1];
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

            //label.set(String.join(":", key.toString(), "!"));
            label.set(key.toString() + ":" + "!");

            int totalFileCount = context.getNumReduceTasks();
            double idfValue = Math.log10(1.0 * 806784 / (fileCount));

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
        System.out.println("Job 1 is running.");
//        FileSystem hdfs = FileSystem.get(conf1);
//        FileStatus[] fileStatuses = hdfs.listStatus(new Path(args[0]));

        Job job1 = Job.getInstance(conf1, "TF");
        job1.setJarByClass(RankedRetrieval.class);

        //job1.setInputFormatClass(KeyValueTextInputFormat.class);

        job1.setMapperClass(TFMapper.class);
        job1.setCombinerClass(TFCombiner.class);
        job1.setReducerClass(TFReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

//        job1.setNumReduceTasks(fileStatuses.length);

        job1.setPartitionerClass(TFPartitioner.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

        // part2----------------------------------------
        Configuration conf2 = new Configuration();
        System.out.println("Job 2 is running.");
        Job job2 = Job.getInstance(conf2, "IDF");
        job2.setJarByClass(RankedRetrieval.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(IDFMapper.class);
        job2.setReducerClass(IDFReducer.class);

//        job2.setNumReduceTasks(fileStatuses.length);

        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);

        // part3----------------------------------------
        Configuration conf3 = new Configuration();
        System.out.println("Job 3 is running.");
        Job job3 = Job.getInstance(conf3);
        job3.setJobName("TF_IDF");
        job3.setJarByClass(RankedRetrieval.class);

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